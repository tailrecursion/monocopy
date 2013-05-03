;; Copyright (c) Alan Dipert. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns tailrecursion.monocopy-test
  (:require [clojure.test            :refer :all                    ]
            [clojure.repl            :refer :all                    ]
            [tailrecursion.monocopy  :refer [datoms hydrate] :as mc ]
            [datomic.api             :refer [q db          ] :as d  ]
            [clojure.pprint          :refer [pprint        ]        ]
            [clojure.data.generators :as    g                       ])
  (:refer-clojure :exclude [rand-int]))

(defn rand-int
  "Replacement of core/rand-int that allows control of the
   randomization basis (through binding clojure.data.generators/*rnd*)."
  [n] (int (g/uniform 0 n)))

(def schema
  [{:db/ident :root/ref
    :db/id #db/id [:db.part/db]
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/ident :root/id
    :db/id #db/id [:db.part/db]
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/value
    :db.install/_attribute :db.part/db}])

(def ^:dynamic *conn*)

(defn using-datomic [f]
  (let [uri "datomic:mem://monocopy"]
    (d/delete-database uri)
    (d/create-database uri)
    (binding [*conn* (d/connect uri)]
      (d/transact *conn* (concat mc/schema schema))
      (f))))

(use-fixtures :each using-datomic)

(let [i (atom 0)]
  (defn root [v]
    (let [eid (d/tempid :db.part/user)]
      (concat [[:db/add eid :root/id (swap! i inc)]]
              (datoms v eid :root/ref)))))

(def magic-n 1000)

(def supported-scalars
  [(constantly nil)
   g/long
   g/double
   g/boolean
   g/string
   g/symbol
   g/keyword
   g/uuid
   g/date])

(def distinct-scalars
  (distinct (map #(%) (cycle supported-scalars))))

(def repeating-scalars
  (cycle (map #(%) supported-scalars)))

(deftest scalars
  (testing "storing scalars"
    (let [randos (take magic-n distinct-scalars)
          txes (mapcat root (range) randos)]
      (d/transact *conn* txes)
      (let [db (d/db *conn*)
            n-tags (ffirst (q '[:find (count ?v) :where [?v :monocopy/tag]] db))
            roots (map (comp (partial d/entity db) first)
                       (q '[:find ?r :where [?r :root/ref]] db))]
        (is (= magic-n n-tags))
        (is (= randos
               (map (comp hydrate :root/ref) (sort-by :root/id roots))))))))

(deftest scalar-hashing
  (testing "scalar hashing"
    (let [repeats (take magic-n repeating-scalars)
          txes (mapcat root (range) repeats)]
      (d/transact *conn* txes)
      (let [db (d/db *conn*)
            n-tags (ffirst (q '[:find (count ?v) :where [?v :monocopy/tag]] db))
            n-roots (ffirst (q '[:find (count ?v) :where [?v :root/id]] db))]
        (assert (> magic-n (count supported-scalars)))
        (is (= magic-n n-roots))
        (is (< n-tags n-roots))))))

(deftest age-query
  (let [people [{:name "Joe"   :age 7  :favs #{:cheese}}
                {:name "Bob"   :age 38 :favs #{:cheese :butter}}
                {:name "Sally" :age 98 :favs #{:cheese :chocolate}}
                {:name "Bob"   :age 7  :favs #{:cheese :butter :chocolate}}]]
    (d/transact *conn* (mapcat root (range) people))
    (let [db (d/db *conn*)
          old-people (map (comp #(update-in % [:root/ref] hydrate)
                                (partial into {})
                                (partial d/entity db)
                                first)
                          (d/q '[:find ?p
                                 :where
                                 [?k1  :monocopy.keyword/value :age]
                                 [?e1  :monocopy.entry/key     ?k1]
                                 [?e1  :monocopy.entry/val     ?v1]
                                 [?v1  :monocopy.long/value    ?age]
                                 [(>= ?age 10)]
                                 [?map :monocopy/entries       ?e1]
                                 [?p   :root/ref               ?map]]
                               db))]
      (is (= #{{:name "Bob"   :age 38 :favs #{:cheese :butter}}
               {:name "Sally" :age 98 :favs #{:cheese :chocolate}}}
             (set (map :root/ref old-people)))))))

(deftest maps-interned
  (let [people [{:name "Joe" :age 7}
                {:name "Joe" :age 7}]]
    (d/transact *conn* (mapcat root (range) people))
    (let [db (d/db *conn*)]
      (is (= 2 (ffirst (q '[:find (count ?e)
                            :where
                            [?e :root/id]] db))))
      (is (= 1 (ffirst (q '[:find (count ?e)
                            :where
                            [?e :monocopy/tag :tailrecursion.monocopy/map]] db)))))))

(defn partitions
  [sizes values]
  (when-let [[size & more] (seq sizes)]
    (when (seq values)
      (lazy-seq (cons (take size values)
                      (partitions more (drop size values)))))))

(defn randmaps
  [sizef depthf values]
  (->> values
       (partitions (repeatedly sizef))
       (partitions (repeatedly depthf))
       (map #(reduce (fn [m [k & rest]]
                       (assoc (apply hash-map (mapcat identity (partition 2 rest))) k m))
                     {}
                     %))))

(comment
  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  (do
    (def uri "datomic:mem://monocopy")
    (d/delete-database uri)
    (d/create-database uri)
    (def conn (d/connect uri))
    (d/transact conn (concat mc/schema schema)))

  (def maps (take magic-n (randmaps #(inc (rand-int 50)) ;size
                                    #(inc (rand-int 10))  ;depth
                                    (map #(%) (repeat g/string)))))
  
  (time
   (doseq [m maps]
     (d/transact conn (root m))))       ;~27s

  (let [start (. System (nanoTime))
        p (promise)
        db (d/db conn)]
    (future
      (->> db
           (q '[:find ?m :where [_ :root/ref ?m]] db)
           (mapcat identity)
           (map #(d/entity db %))
           (mapv hydrate))
      (deliver p (- (. System (nanoTime)) start)))
    (println (str "Elapsed: " (/ (double @p) 1000000.0))))
  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  )
