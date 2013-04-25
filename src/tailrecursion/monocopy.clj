;; Copyright (c) Alan Dipert. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns tailrecursion.monocopy
  (:require [datomic.api :refer [q db] :as d]))

(def schema
  [;; scalars
   {:db/ident :monocopy.keyword/value
    :db/id #db/id [:db.part/db]
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db.install/_attribute :db.part/db}
   {:db/ident :monocopy.string/value
    :db/id #db/id [:db.part/db]
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db.install/_attribute :db.part/db}
   {:db/ident :monocopy.boolean/value
    :db/id #db/id [:db.part/db]
    :db/valueType :db.type/boolean
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db.install/_attribute :db.part/db}
   {:db/ident :monocopy.long/value
    :db/id #db/id [:db.part/db]
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db.install/_attribute :db.part/db}
   {:db/ident :monocopy.double/value
    :db/id #db/id [:db.part/db]
    :db/valueType :db.type/double
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db.install/_attribute :db.part/db}
   {:db/ident :monocopy.instant/value
    :db/id #db/id [:db.part/db]
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db.install/_attribute :db.part/db}
   {:db/ident :monocopy.uuid/value
    :db/id #db/id [:db.part/db]
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db.install/_attribute :db.part/db}
   {:db/ident :monocopy.uri/value
    :db/id #db/id [:db.part/db]
    :db/valueType :db.type/uri
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db.install/_attribute :db.part/db}
   {:db/ident :monocopy.symbol/value
    :db/id #db/id [:db.part/db]
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db.install/_attribute :db.part/db}
   ;; entries
   {:db/ident :monocopy.entry/hashCode
    :db/id #db/id [:db.part/db]
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db.install/_attribute :db.part/db}
   {:db/ident :monocopy.entry/key
    :db/id #db/id [:db.part/db]
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/ident :monocopy.entry/val
    :db/id #db/id [:db.part/db]
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   ;;maps
   {:db/ident :monocopy.map/hashCode
    :db/id #db/id [:db.part/db]
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db.install/_attribute :db.part/db}
   {:db/ident :monocopy.map/entries
    :db/id #db/id [:db.part/db]
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db.install/_attribute :db.part/db}
   ;;vectors
   {:db/ident :monocopy.vector/hashCode
    :db/id #db/id [:db.part/db]
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db.install/_attribute :db.part/db}
   {:db/ident :monocopy.vector/entries
    :db/id #db/id [:db.part/db]
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db.install/_attribute :db.part/db}
   ;;lists
   {:db/ident :monocopy.list/hashCode
    :db/id #db/id [:db.part/db]
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db.install/_attribute :db.part/db}
   {:db/ident :monocopy.list/entries
    :db/id #db/id [:db.part/db]
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db.install/_attribute :db.part/db}
   ;; sets
   {:db/ident :monocopy.set/hashCode
    :db/id #db/id [:db.part/db]
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db.install/_attribute :db.part/db}
   {:db/ident :monocopy.set/members
    :db/id #db/id [:db.part/db]
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db/index true
    :db.install/_attribute :db.part/db}])

(defprotocol Hashcons
  (datoms [this pid pattr]))

(defn map->datoms
  [m type pid pattr]
  (let [map-id (d/tempid :db.part/user)
        entries-attr (keyword (name type) "entries")]
    (concat
     [[:db/add map-id :monocopy.map/hashCode (hash m)]]
     (mapcat #(datoms % map-id entries-attr) m)
     [[:db/add pid pattr map-id]])))

(defn entry->datoms
  [[k v :as entry] pid pattr]
  (let [id (d/tempid :db.part/user)]
    (concat
     [[:db/add id :monocopy.entry/hashCode (hash entry)]]
     (datoms k id :monocopy.entry/key)
     (datoms v id :monocopy.entry/val)
     [[:db/add pid pattr id]])))

(defn scalar->datoms [v type pid pattr]
  (let [id (d/tempid :db.part/user)
        value-attr (keyword (name type) "value")]
    [[:db/add id value-attr v]
     [:db/add pid pattr id]]))

(extend-protocol Hashcons
  ;; scalars
  clojure.lang.Keyword
  (datoms [this pid pattr]
    (scalar->datoms this :monocopy.keyword pid pattr))
  String
  (datoms [this pid pattr]
    (scalar->datoms this :monocopy.string pid pattr))
  Boolean
  (datoms [this pid pattr]
    (scalar->datoms this :monocopy.boolean pid pattr))
  Long
  (datoms [this pid pattr]
    (scalar->datoms this :monocopy.long pid pattr))
  Double
  (datoms [this pid pattr]
    (scalar->datoms this :monocopy.double pid pattr))
  java.util.Date
  (datoms [this pid pattr]
    (scalar->datoms this :monocopy.instant pid pattr))
  java.util.UUID
  (datoms [this pid pattr]
    (scalar->datoms this :monocopy.uuid pid pattr))
  java.net.URI
  (datoms [this pid pattr]
    (scalar->datoms this :monocopy.uri pid pattr))
  clojure.lang.Symbol
  (datoms [this pid pattr]
    (let [id (d/tempid :db.part/user)]
      [[:db/add id :monocopy.symbol/value (str this)]
       [:db/add pid pattr id]]))

  ;; collections
  clojure.lang.MapEntry
  (datoms [this pid pattr]
    (entry->datoms this pid pattr))
  clojure.lang.PersistentVector
  (datoms [this pid pattr]
    (map->datoms (zipmap (range) this) :monocopy.vector pid pattr))
  clojure.lang.PersistentList
  (datoms [this pid pattr]
    (map->datoms (zipmap (range) this) :monocopy.list pid pattr))
  clojure.lang.LazySeq
  (datoms [this pid pattr]
    (datoms (apply list this) pid pattr))
  clojure.lang.PersistentList$EmptyList
  (datoms [this pid pattr]
    (let [id (d/tempid :db.part/user)]
      [[:db/add id :monocopy.list/hashCode (hash this)]
       [:db/add pid pattr id]]))
  clojure.lang.PersistentArrayMap
  (datoms [this pid pattr]
    (map->datoms this :monocopy.map pid pattr))
  clojure.lang.PersistentHashMap
  (datoms [this pid pattr]
    (map->datoms this :monocopy.map pid pattr))
  clojure.lang.PersistentHashSet
  (datoms [this pid pattr]
    (let [id (d/tempid :db.part/user)]
      (concat
       [[:db/add id :monocopy.set/hashCode (hash this)]]
       (mapcat #(datoms % id :monocopy.set/members) this)
       [[:db/add pid pattr id]]))))

(defn entity-type [e]
  (-> e keys first namespace keyword))

(defmulti hydrate entity-type)

(defn hydrate-maplike [type e]
  (reduce (fn [m entry]
            (let [[k v] (map (comp hydrate (partial get entry))
                             [:monocopy.entry/key :monocopy.entry/val])]
              (assoc m k v)))
          {}
          (get e (keyword (name type) "entries"))))

(defmethod hydrate :monocopy.list [e]
  (->> (hydrate-maplike :monocopy.list e)
       (sort-by key)
       (map second)
       (apply list)))

(defmethod hydrate :monocopy.vector [e]
  (->> (hydrate-maplike :monocopy.vector e)
       (sort-by key)
       (mapv second)))

(defmethod hydrate :monocopy.map [e]
  (hydrate-maplike :monocopy.map e))

(defmethod hydrate :monocopy.set [e]
  (->> (get e :monocopy.set/members)
       (map hydrate)
       set))

(defmethod hydrate :monocopy.symbol [e]
  (symbol (get e :monocopy.symbol/value)))

(defmethod hydrate :default [e]
  (let [value-attr (keyword (name (entity-type e)) "value")]
    (get e value-attr)))
