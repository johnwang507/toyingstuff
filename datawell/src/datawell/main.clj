(ns datawell.main
  (:use [clojure.tools.cli :only [cli]]
        [clojure.java.io :only [file]]
        [clojure.string :only [join]]
        [datawell.gen.plain])
  (:gen-class))

(def ^:private gens ; All available generators in the "gen" folder
  (->> "gens"
      file
      .listFiles
      (map #(.getName %))
      (filter #(re-matches #".*\.clj" %))
      (map #(drop-last 4 %))
      (map #(apply str %))))

(map #(load (str "gen/" %)) gens) ; Load all generators

(defn- pargs [args]
  (cli args
    ["-h" "--help" "Show help. If a generator is specified, show options for that generator"]
    ["-g" "--generator" (str "The generator to use for generating records. Available: " (join "," gens)) :default "plain"]))

(defn- run
  "Print out the options and the arguments"
  [opts args]
  (println (str "Options:\n" opts))
  (println (str "Arguments:\n" args)))

(defn -main [& args]
  (let [[opts args banner] (pargs args)]
    (when (:help opts)
      (println banner)
      (System/exit 0))
    (if (every? empty? [args opts])
      (println banner)
      (do (run opts args)))))
