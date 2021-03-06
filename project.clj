(defproject tailrecursion/monocopy "1.0.10-SNAPSHOT"
  :description "Schema and library for storing Clojure data in Datomic"
  :url "https://github.com/tailrecursion/monocopy"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [com.datomic/datomic-free "0.8.3889"]
                 [commons-codec "1.7"]
                 [org.clojure/data.generators "0.1.2" :scope "test"]]
  :main ^:skip-aot tailrecursion.monocopy-test/bench)
