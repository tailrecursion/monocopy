;; Copyright (c) Alan Dipert. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns tailrecursion.monocopy
  (:require [datomic.api :refer [q db] :as d]
            [clojure.walk :refer [postwalk]]))

(def schema
  [;; housekeeping
   {:db/ident :monocopy/tag
    :db/id #db/id [:db.part/db]
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/ident :monocopy/md5
    :db/id #db/id [:db.part/db]
    :db/valueType :db.type/string
    :db/index true
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
    :db.install/_attribute :db.part/db}])

(defprotocol Hashcons
  (tag [this])
  (datoms [this pid pattr]))

(defn value-attr [tag]
  (keyword (str "monocopy." (name tag)) "value"))

(defn md5 [form]
  (->> form
       pr-str
       .getBytes
       (.digest (java.security.MessageDigest/getInstance "MD5"))
       org.apache.commons.codec.binary.Base64/encodeBase64String))

(defn map->datoms
  [m tag pid pattr]
  (let [map-id (d/tempid :db.part/user)]
    (concat
     [[:db/add map-id :monocopy/md5 (md5 m)]
      [:db/add map-id :monocopy/tag tag]]
     (mapcat #(datoms % map-id :monocopy/entries) m)
     [[:db/add pid pattr map-id]])))

(defn entry->datoms
  [[k v :as entry] pid pattr]
  (let [id (d/tempid :db.part/user)]
    (concat
     [[:db/add id :monocopy/tag ::entry]
      [:db/add id :monocopy/md5 (md5 entry)]]
     (datoms k id :monocopy.entry/key)
     (datoms v id :monocopy.entry/val)
     [[:db/add pid pattr id]])))

(defn scalar->datoms [v tag pid pattr]
  (let [id (d/tempid :db.part/user)]
    [[:db/add id :monocopy/tag    tag]
     [:db/add id (value-attr tag) v]
     [:db/add pid pattr           id]]))

(extend-protocol Hashcons
  ;; scalars
  clojure.lang.Keyword
  (tag [_] ::keyword)
  (datoms [this pid pattr]
    (scalar->datoms this (tag this) pid pattr))
  String
  (tag [_] ::string)
  (datoms [this pid pattr]
    (scalar->datoms this (tag this) pid pattr))
  Boolean
  (tag [_] ::boolean)
  (datoms [this pid pattr]
    (scalar->datoms this (tag this) pid pattr))
  Long
  (tag [_] ::long)
  (datoms [this pid pattr]
    (scalar->datoms this (tag this) pid pattr))
  Double
  (tag [_] ::double)
  (datoms [this pid pattr]
    (scalar->datoms this (tag this) pid pattr))
  java.util.Date
  (tag [_] ::instant)
  (datoms [this pid pattr]
    (scalar->datoms this (tag this) pid pattr))
  java.util.UUID
  (tag [_] ::uuid)
  (datoms [this pid pattr]
    (scalar->datoms this (tag this) pid pattr))
  java.net.URI
  (tag [_] ::uri)
  (datoms [this pid pattr]
    (scalar->datoms this (tag this) pid pattr))
  clojure.lang.Symbol
  (tag [_] ::symbol)
  (datoms [this pid pattr]
    (scalar->datoms (str this) (tag this) pid pattr))
  nil
  (tag [_] ::nil)
  (datoms [this pid pattr]
    (let [id (d/tempid :db.part/user)]
      [[:db/add id  :monocopy/md5 (md5 this)]
       [:db/add id  :monocopy/tag (tag this)]
       [:db/add pid pattr         id]]))
  ;; collections
  clojure.lang.MapEntry
  (tag [_] ::entry)
  (datoms [this pid pattr]
    (entry->datoms this pid pattr))
  clojure.lang.PersistentArrayMap
  (tag [_] ::map)
  (datoms [this pid pattr]
    (map->datoms this (tag this) pid pattr))
  clojure.lang.PersistentHashMap
  (tag [_] ::map)
  (datoms [this pid pattr]
    (map->datoms this (tag this) pid pattr)))

;;; hydrate

(defmulti hydrate :monocopy/tag)

(deftype LazyMap [db e]
  clojure.lang.ILookup
  (valAt [this k]
    (.valAt this k nil))
  (valAt [this k not-found]
    (if-let [vid (ffirst (q '{:find  [?v]
                              :in    [$ ?vattr ?k ?m]
                              :where [[?key ?vattr              ?k]
                                      [?e   :monocopy.entry/key ?key]
                                      [?m   :monocopy/entries   ?e]
                                      [?e   :monocopy.entry/val ?v]]}
                            db (-> k tag value-attr) k (:db/id e)))]
      (hydrate (d/entity db vid))
      not-found))
  clojure.lang.IPersistentCollection
  (count [_]
    (count (get e :monocopy/entries)))
  (equiv [_ o]
    (and (isa? (class o) LazyMap)
         (= db (.-db o))
         (= (:db/id e) (:db/id (.-e o)))))
  clojure.lang.Seqable
  (seq [_]
    (->> (get e :monocopy/entries)
         (map (fn [entry]
                (mapv (comp hydrate (partial get entry))
                      [:monocopy.entry/key :monocopy.entry/val]))))))

(defmethod hydrate ::map [e]
  (LazyMap. (d/entity-db e) e))

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
