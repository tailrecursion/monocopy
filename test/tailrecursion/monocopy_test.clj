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
            [clojure.data.generators :as    g])
  (:refer-clojure :exclude [rand-int]))

(def schema
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
      (d/transact *conn* (concat mc/schema schema))
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

(def distinct-scalars
  (distinct (map #(%) (cycle supported-scalars))))

(def repeating-scalars
  (cycle (map #(%) supported-scalars)))
