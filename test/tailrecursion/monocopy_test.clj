;; Copyright (c) Alan Dipert. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns tailrecursion.monocopy-test
  (:require [clojure.test :refer :all]
            [tailrecursion.monocopy :refer [datoms hydrate] :as mc]
            [datomic.api :refer [q db] :as d]))

(def ^:dynamic *conn*)

(def schema
  [{:db/ident :person/ref
    :db/id #db/id [:db.part/db]
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/ident :person/id
    :db/id #db/id [:db.part/db]
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db.install/_attribute :db.part/db}])

(defn datomic-fixture [f]
  (let [uri "datomic:mem://monocopy"]
    (d/delete-database uri)
    (d/create-database uri)
    (binding [*conn* (d/connect uri)]
      (d/transact *conn* (concat mc/schema schema))
      (f))))

(use-fixtures :once datomic-fixture)

(deftest age-query
  (let [people [{:name "Joe"   :age 7  :favs #{:cheese}}
                {:name "Bob"   :age 38 :favs #{:cheese :butter}}
                {:name "Sally" :age 98 :favs #{:cheese :chocolate}}
                {:name "Bob"   :age 7  :favs #{:cheese :butter :chocolate}}]]
    (d/transact *conn*
                (mapcat #(let [id (d/tempid :db.part/user)]
                           (concat
                            [[:db/add id :person/id (java.util.UUID/randomUUID)]]
                            (datoms % id :person/ref))) people))
    (let [db (d/db *conn*)
          old-people (map (comp #(update-in % [:person/ref] hydrate)
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
                                 [?map :monocopy.map/entries   ?e1]
                                 [?p   :person/ref             ?map]]
                               db))]
      (is (= #{{:name "Bob"   :age 38 :favs #{:cheese :butter}}
               {:name "Sally" :age 98 :favs #{:cheese :chocolate}}}
             (set (map :person/ref old-people)))))))
