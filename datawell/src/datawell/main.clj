(ns datawell.main
  (:use [clojure.tools.cli :only [cli]]
        [clojure.java.io :only [file]])
  (:gen-class))


(defn- pint [x] (Integer. x))

(defn- pargs [args]
  (cli args
    ["-h" "--help" "Show help" :flag true :default false]
    ["-n" "--colnum" "How many columns in a row" :parse-fn pint :default 9]
    ["-s" "--colsize" "How many characters can a column hold, e.g., the column size." :parse-fn pint :default 15]
    ["-f" "--fixedlen" "Should the size of every column be the same" :flag true :default true]
    ["-a" "--allchars" "Use all visiable characters to generate data. Otherwise only alphabet will be used" :flag true :default true]))

(defn- gen-col
  "Compose a string with no more than *maxlen* long, by randomly picking characters from the *seeds*.
  If *fixedlen* is true, the strings generated every times will all be *maxlen* long, 
  otherwise their length are random. The *seeds* is a sequence of Integers corresponding to ASCII characters."
  [seeds maxlen fixedlen]
  (let [len (if fixedlen
    maxlen
    (inc (rand-int maxlen)))]
  (apply str (repeatedly len #(->> seeds (map char) rand-nth)))))

(defn- spout [idx pre {:keys [colnum colsize fixedlen alphabet]} args]
  (let [seeds (if alphabet
    (concat (range 65 91) (range 97 123))
    (range 32 127))]
  (repeatedly colnum #(gen-col seeds colsize fixedlen))))


(defn -main [& args]
  (let [[opts args banner] (pargs args)]
    (when (:help opts)
      (println banner)
      (System/exit 0))
    (if (every? empty? [args opts])
      (println banner)
      (do (println opts args)))))
