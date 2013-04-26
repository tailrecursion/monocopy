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
  [;; housekeeping
   {:db/ident :monocopy/tag
    :db/id #db/id [:db.part/db]
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/ident :monocopy/hashCode
    :db/id #db/id [:db.part/db]
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db.install/_attribute :db.part/db}
   {:db/ident :monocopy/entries
    :db/id #db/id [:db.part/db]
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db.install/_attribute :db.part/db}
   ;; scalars
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
   ;; sets
   {:db/ident :monocopy.set/members
    :db/id #db/id [:db.part/db]
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db/index true
    :db.install/_attribute :db.part/db}])

(defprotocol Hashcons
  (datoms [this pid pattr]))

(defn value-attr [tag]
  (keyword (str "monocopy." (name tag)) "value"))

(defn map->datoms
  [m tag pid pattr]
  (let [map-id (d/tempid :db.part/user)]
    (concat
     [[:db/add map-id :monocopy/hashCode (hash m)]
      [:db/add map-id :monocopy/tag      tag]]
     (mapcat #(datoms % map-id :monocopy/entries) m)
     [[:db/add pid pattr map-id]])))

(defn entry->datoms
  [[k v :as entry] pid pattr]
  (let [id (d/tempid :db.part/user)]
    (concat
     [[:db/add id :monocopy/hashCode (hash entry)]
      [:db/add id :monocopy/tag      ::entry]]
     (datoms k id :monocopy.entry/key)
     (datoms v id :monocopy.entry/val)
     [[:db/add pid pattr id]])))

(defn scalar->datoms [v tag pid pattr]
  (let [id (d/tempid :db.part/user)]
    [[:db/add id  :monocopy/tag    tag]
     [:db/add id  (value-attr tag) v]
     [:db/add pid pattr            id]]))

(extend-protocol Hashcons
  ;; scalars
  clojure.lang.Keyword
  (datoms [this pid pattr]
    (scalar->datoms this ::keyword pid pattr))
  String
  (datoms [this pid pattr]
    (scalar->datoms this ::string pid pattr))
  Boolean
  (datoms [this pid pattr]
    (scalar->datoms this ::boolean pid pattr))
  Long
  (datoms [this pid pattr]
    (scalar->datoms this ::long pid pattr))
  Double
  (datoms [this pid pattr]
    (scalar->datoms this ::double pid pattr))
  java.util.Date
  (datoms [this pid pattr]
    (scalar->datoms this ::instant pid pattr))
  java.util.UUID
  (datoms [this pid pattr]
    (scalar->datoms this ::uuid pid pattr))
  java.net.URI
  (datoms [this pid pattr]
    (scalar->datoms this ::uri pid pattr))
  clojure.lang.Symbol
  (datoms [this pid pattr]
    (scalar->datoms (str this) ::symbol pid pattr))
  nil
  (datoms [this pid pattr]
    (let [id (d/tempid :db.part/user)]
      [[:db/add id  :monocopy/hashCode (hash this)]
       [:db/add id  :monocopy/tag      ::nil]
       [:db/add pid pattr              id]]))
  ;; collections
  clojure.lang.MapEntry
  (datoms [this pid pattr]
    (entry->datoms this pid pattr))
  clojure.lang.PersistentVector
  (datoms [this pid pattr]
    (map->datoms (zipmap (range) this) ::vector pid pattr))
  clojure.lang.PersistentList
  (datoms [this pid pattr]
    (map->datoms (zipmap (range) this) ::list pid pattr))
  clojure.lang.PersistentList$EmptyList
  (datoms [this pid pattr]
    (let [id (d/tempid :db.part/user)]
      [[:db/add id  :monocopy/hashCode (hash this)]
       [:db/add id  :monocopy/tag      ::list]
       [:db/add pid pattr              id]]))
  clojure.lang.PersistentArrayMap
  (datoms [this pid pattr]
    (map->datoms this ::map pid pattr))
  clojure.lang.PersistentHashMap
  (datoms [this pid pattr]
    (map->datoms this ::map pid pattr))
  clojure.lang.PersistentHashSet
  (datoms [this pid pattr]
    (let [id (d/tempid :db.part/user)]
      (concat
       [[:db/add id :monocopy/hashCode (hash this)]
        [:db/add id :monocopy/tag      ::set]]
       (mapcat #(datoms % id :monocopy.set/members) this)
       [[:db/add pid pattr id]]))))

(defmulti hydrate :monocopy/tag)

(defn hydrate-map [e]
  (reduce #(conj %1 (hydrate %2)) {} (get e :monocopy/entries)))

(defmethod hydrate ::list [e]
  (->> (hydrate-map e)
       (sort-by key)
       (map second)
       (apply list)))

(defmethod hydrate ::vector [e]
  (->> (hydrate-map e)
       (sort-by key)
       (mapv second)))

(defmethod hydrate ::entry [e]
  (mapv #(hydrate (get e %))
        [:monocopy.entry/key :monocopy.entry/val]))

(defmethod hydrate ::map [e]
  (hydrate-map e))

(defmethod hydrate ::set [e]
  (->> (get e :monocopy.set/members)
       (map hydrate)
       set))

(defmethod hydrate ::symbol [e]
  (symbol (get e :monocopy.symbol/value)))

(defmethod hydrate ::nil [e])

(derive ::keyword ::scalar)
(derive ::string  ::scalar)
(derive ::boolean ::scalar)
(derive ::long    ::scalar)
(derive ::double  ::scalar)
(derive ::instant ::scalar)
(derive ::uuid    ::scalar)
(derive ::uri     ::scalar)

(defmethod hydrate ::scalar [e]
  (get e (value-attr (:monocopy/tag e))))
