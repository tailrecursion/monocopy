;; Copyright (c) Alan Dipert. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns tailrecursion.monocopy-test
  (:require [clojure.test            :refer :all]
            [clojure.repl            :refer :all]
            [tailrecursion.monocopy  :refer [datoms hydrate] :as mc]
            [datomic.api             :refer [q db] :as d]
            [clojure.pprint          :refer [pprint]]
            [clojure.data.generators :as    g]))

(def test-schema
  [{:db/ident :root/ref
    :db/id #db/id [:db.part/db]
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/ident :root/id
    :db/id #db/id [:db.part/db]
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/value
    :db.install/_attribute :db.part/db}])

(def ^:dynamic *conn*)
(def ^:dynamic *magic* 1000)

(use-fixtures :each (fn [f]
                      (let [test-uri "datomic:mem://monocopy"]
                        (d/delete-database test-uri)
                        (d/create-database test-uri)
                        (binding [*conn* (d/connect test-uri)]
                          (d/transact *conn* (concat mc/schema test-schema))
                          (f)))))

(defn root [v]
  (let [eid (d/tempid :db.part/user)]
    (concat [[:db/add eid :root/id (java.util.UUID/randomUUID)]]
            (datoms v eid :root/ref))))

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

(defn cycle-shuffle [coll]
  (lazy-seq
   (when-let [s (seq coll)]
     (concat (g/shuffle s) (cycle-shuffle s)))))

(def scalars-seq
  (map #(%) (cycle-shuffle supported-scalars)))

(defn randmaps [src]
  (map (partial apply hash-map) (partition 10 src)))

(deftest par-map-insertion
  (let [maps (vec (take *magic* (randmaps scalars-seq)))
        txes (pmap (comp #(d/transact-async *conn* %) root) maps)]
    (doseq [tx txes] (deref tx))
    (let [db (d/db *conn*)
          [[n-roots]] (q '[:find (count ?e) :where [?e :root/id]] db)
          [[n-disti]] (q '[:find (count ?r) :where [_ :root/ref ?r]] db)
          [[n-entri]] (q '[:find (count ?e)
                           :where
                           [?e :monocopy/tag :tailrecursion.monocopy/entry]] db)
          [[n-tagged]] (q '[:find (count ?e) :where [?e :monocopy/tag]] db)]
      (is (= n-roots (count maps)))
      (is (= n-disti (count (set maps))))
      (is (= n-entri (count (set (mapcat identity maps)))))
      (is (= n-tagged
             (+ (count (set (mapcat (partial mapcat identity) maps)))
                n-disti
                n-entri))))))

(deftest map-hydration
  (let [maps (take *magic* (randmaps scalars-seq))]
    (d/transact *conn* (mapcat root maps))
    (let [db (d/db *conn*)
          eids (mapcat identity (q '[:find ?ref :where [_ :root/ref ?ref]] db))]
      (is (= (set maps)
             (set (map (comp (partial into {})
                             hydrate
                             #(d/entity db %))
                       eids)))))))

;;; perf:
;;; lein run :uri "datomic:mem://monocopy" :magic 5000 :iters 10

(defmacro dotimed [[binding n] & body]
  `(mapv (fn [i#]
           (let [~binding i#
                 start# (System/nanoTime)
                 ret# (do ~@body)]
             (/ (double (- (System/nanoTime) start#)) 1000000.0)))
         (range ~n)))

(defn ^:perf map-insertion-perf [iters]
  (let [maps (take *magic* (randmaps scalars-seq))]
    (dotimed [_ iters]
      (d/transact *conn* (mapcat root maps)))))

(defn ^:perf map-hydration-perf [iters]
  (let [maps (take *magic* (randmaps scalars-seq))]
    (d/transact *conn* (mapcat root maps))
    (let [db (d/db *conn*)]
      (dotimed [_ iters]
        (let [eids (mapcat identity (q '[:find ?ref :where [_ :root/ref ?ref]] db))]
          (mapv (comp (partial into {})
                      hydrate
                      #(d/entity db %)) eids))))))

(defn benchf [magic uri iters f]
  (d/delete-database uri)
  (d/create-database uri)
  (binding [*magic* magic
            *conn* (d/connect uri)]
    (d/transact *conn* (concat mc/schema test-schema))
    (f iters)))

(defn bench
  [& {:keys [magic uri iters]
      :or {magic 1000, uri "datomic:mem://monocopy", iters 5}}]
  (let [perfns  (->> (ns-publics (the-ns 'tailrecursion.monocopy-test))
                     (filter (comp :perf meta second)))
        focused (seq (filter (comp :focus meta second) perfns))]
    (->> (or focused perfns)
         (map (fn [[name f]] [name (benchf magic uri iters f)]))
         (into {})
         pprint)))
