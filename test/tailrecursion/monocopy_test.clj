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

(defn datomically [f]
  (let [uri "datomic:mem://monocopy"]
    (d/delete-database uri)
    (d/create-database uri)
    (binding [*conn* (d/connect uri)]
      (d/transact *conn* (concat mc/schema test-schema))
      (f))))

(use-fixtures :each datomically)

(defn root [v]
  (let [eid (d/tempid :db.part/user)]
    (concat [[:db/add eid :root/id (java.util.UUID/randomUUID)]]
            (datoms v eid :root/ref))))

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

(def scalars-seq
  (cycle (map #(%) supported-scalars)))

(defn randmaps [src]
  (map (partial apply hash-map) (partition 10 src)))

(deftest par-map-insertion
  (let [maps (vec (take magic-n (randmaps scalars-seq)))
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

