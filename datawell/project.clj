(defproject datawell "0.1.0-SNAPSHOT"
  :description "A tool emitting data records. It can be used to generate test data."
  :url "https://github.com/johnwang507/toyingstuff/tree/master/datawell.clj"
  :license {:name "Eclipse Public License"
  :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main datawell.main 
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.cli "0.2.2"]
                 [ring/ring-core "1.2.0"]
                 [ring/ring-jetty-adapter "1.2.0"]]
  :profiles {:dev {:dependencies [[midje "1.5.1"]]}})