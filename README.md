# monocopy

monocopy is a library and schema for efficiently storing many Clojure
value types in a [Datomic](http://www.datomic.com/) database.

Supported types include strings, symbols, keywords, doubles, longs,
sets, maps, lists, and vectors.

monocopy represents most collections as sets of pairs.  In order to
make storage and query as efficient as possible, only distinct
collections, pairs, and scalars are stored.

### Dependency [![Build Status](https://travis-ci.org/tailrecursion/monocopy.png?branch=master)](https://travis-ci.org/tailrecursion/monocopy)

```clojure
[tailrecursion/monocopy "1.0.8"]
```

monocopy depends on `com.datomic/datomic-free`.  To use it in a project that depends on `com.datomic/datomic-pro`, your dependency would be:

```clojure
[tailrecursion/monocopy "1.0.8" :exclusions [com.datomic/datomic-free]]
```

## Example

```clojure
(require '[tailrecursion.monocopy :refer [datoms hydrate] :as mc])
(require '[datomic.api            :refer [q db]           :as d])

;; create, connect to an in-memory database
(def uri "datomic:mem://monocopy")
(d/create-database uri)
(def conn (d/connect uri))

;; load in the monocopy schema
(d/transact conn mc/schema)

;; load in your application schema
(d/transact conn [{:db/doc "Attribute pointing to some monocopy data structure"
                   :db/ident :person/ref
                   :db/id #db/id [:db.part/db]
                   :db/valueType :db.type/ref
                   :db/cardinality :db.cardinality/one
                   :db.install/_attribute :db.part/db}
                  {:db/doc "Uniquely identifies this person"
                   :db/ident :person/id
                   :db/id #db/id [:db.part/db]
                   :db/valueType :db.type/uuid
                   :db/cardinality :db.cardinality/one
                   :db/unique :db.unique/identity
                   :db.install/_attribute :db.part/db}])

(def bob {:name "Bob" :age 39})

;; add Bob
(d/transact conn
            (let [id (d/tempid :db.part/user)]
              (concat [[:db/add id :person/id (java.util.UUID/randomUUID)]]
                      ;; datoms takes a value, parent entity id,
                      ;; and parent attribute to attach to
                      (datoms bob id :person/ref))))

;; query to find people named Bob
(def query
  '[:find ?person
    :where
    [?k1     :monocopy.keyword/value :name]
    [?e1     :monocopy.entry/key     ?k1]
    [?e1     :monocopy.entry/val     ?v1]
    [?v1     :monocopy.string/value  "Bob"]
    [?map    :monocopy/entries       ?e1]
    [?person :person/ref             ?map]])

;; find people named Bob
(let [db (d/db conn)]
  (map (comp #(update-in % [:person/ref] hydrate)
             (partial into {})
             (partial d/entity db)
             first)
       (d/q query db)))
```

## Notes

monocopy uses the md5 hash of the printed value of collections as
unique identifiers, so there is a possibility of hash collision and
data loss.

Ultimately, we'd like to implement this [Dynamic Perfect
Hashing](http://www.arl.wustl.edu/~sailesh/download_files/Limited_Edition/hash/Dynamic%20Perfect%20Hashing-%20Upper%20and%20Lower%20Bounds.pdf)
scheme and deal with collisions such that data is not lost.

## License

    Copyright (c) Alan Dipert. All rights reserved.
    The use and distribution terms for this software are
    covered by the Eclipse Public License 1.0
    (http://opensource.org/licenses/eclipse-1.0.php) which can be
    found in the file epl-v10.html at the root of this
    distribution. By using this software in any fashion, you are
    agreeing to be bound by the terms of this license. You must not
    remove this notice, or any other, from this software.
