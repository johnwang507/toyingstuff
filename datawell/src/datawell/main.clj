(ns datawell.main
  (:use [clojure.tools.cli :only [cli]]
        [clojure.java.io :only [file]]
        [clojure.string :only [join]]
        [ring.adapter.jetty :only [run-jetty]]
        [ring.util.response :only [response]]
        [clojure.contrib.server-socket])
  (:import [java.io PrintWriter])
  (:gen-class))

(defn- gen-col
  "Compose a string with no more than *maxlen* long, by randomly picking characters from the *seeds*.
  If *fixedlen* is true, all strings generated will be of the same *maxlen* long, 
  otherwise their length are random. The *seeds* is a sequence of Integers of ASCII characters."
  [seeds maxlen fixedlen]
  (let [len (if fixedlen maxlen (inc (rand-int maxlen)))]
  (apply str (repeatedly len #(->> seeds (map char) rand-nth)))))

(defn spout [{:keys [colnum colength fixedlen words seperator]}]
  (let [base (if words (range 97 123) (range 32 127))
        sep (-> seperator first int)
        seeds (remove #(= sep %) base)]
      (join seperator (repeatedly colnum #(gen-col seeds colength fixedlen)))))

(defn emit [opts fn-send fn-clean]
  (let [{:keys [maxnum interval]} opts]
    (loop [x 0N]
      (if (< x maxnum)
          (do (-> opts spout fn-send) (Thread/sleep interval) (recur (inc x)))
          (if (ifn? fn-clean) (fn-clean))))))

(defn svc-http [opts]
    (run-jetty (fn [request] (response (spout opts))) {:port (:port opts)}))

(def pemit #(emit % println nil))

(defn svc-stdout [opts] (pemit opts))

(defn svc-socket [opts]
  (create-server (:port opts) (fn [_ out] (binding [*out* (PrintWriter. out)] (pemit opts)))))

(def svc-type [1 #'svc-stdout 2 #'svc-http 3 #'svc-socket])

(defn- pargs [args]
  (let [pint #(bigint %)
    type-opts (->> svc-type (take-nth 2) (join ", "))
    type-nms (->> svc-type rest (take-nth 2) (map #(-> % meta :name name (.substring 4))) (join ", "))]
  (cli args
    ["-h" "--help" "Show this help." :flag true :default false]
    ["-t" "--type" (str "The type of the service. Avaliable: " type-opts ", standing for " type-nms ".") :parse-fn pint :default 1]
    ["-p" "--port" "The port to bind" :parse-fn pint :default 9876]
    ["-i" "--interval" "The time interval between two consecutive records being generated, in millisecond. Ignored if the type of service is pull-style(e.g., http) instead of push-style(e.g., stdout, socket)." :parse-fn pint :default 200]
    ["-m" "--maxnum" "The max number of records to be generated in push-style server(e.g., stdout, socket). After that the server will stop pushing records" :parse-fn pint :default 5]
    ["-n" "--colnum" "How many columns in a row." :parse-fn pint :default 9]
    ["-l" "--colength" "How many characters can a column hold, e.g., the column size." :parse-fn pint :default 15]
    ["-f" "--fixedlen" "Should the size of every column be the same." :flag true :default true]
    ["-s" "--seperator" "What character will be the seperator for columns. This character will not be in characters seeds." :default "\t"]
    ["-w" "--words" "Use only lowcase alphabet character as seeds to generate random string. Otherwise all visiable characters will be used." :flag true :default true])))

(defn -main [& args]
  (let [[opts args banner] (pargs args)]
    (when (:help opts)
      (println banner)
      (System/exit 0))
    (let [svc (->> opts :type (.indexOf svc-type) inc (nth svc-type))]
      (reset! roof (:maxnum opts))
      (meta svc))))

