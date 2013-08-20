(ns datawell.main
  (:use [clojure.tools.cli :only [cli]]
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

(defn gen-row [{:keys [colnum colength fixedlen words seperator]}]
  (let [base (if words (range 97 123) (range 32 127))
        sep (-> seperator first int)
        seeds (remove #(= sep %) base)]
      (join seperator (repeatedly colnum #(gen-col seeds colength fixedlen)))))

(defn print-rows [opts]
  (let [{:keys [maxnum interval]} opts]
    (loop [x 0N]
      (if (< x maxnum)
          (do (-> opts gen-row println) (Thread/sleep interval) (recur (inc x)))))))

(defn svc-http [opts]
    (run-jetty (fn [request] (response (gen-row opts))) {:port (:port opts)}))

(defn svc-stdout [opts] (print-rows opts))

(defn svc-socket [opts]
  (create-server (:port opts) (fn [_ out] (binding [*out* (PrintWriter. out)] (print-rows opts)))))

(def svc-type ["stdout" "http" "socket"])

(defn- pargs [args]
  (let [pint #(bigint %)]
    (cli args
      ["-h" "--help" "Show this help." :flag true :default false]
      ["-t" "--type" (str "The type of the service. Avaliable: " svc-type) :default (first svc-type)]
      ["-p" "--port" "The port to bind, used in network service(e.g., http, socket)" :parse-fn pint :default 9876]
      ["-i" "--interval" "The time interval between two consecutive records being generated, in millisecond. Ignored if the type of service is pull-style(e.g., http) instead of push-style(e.g., stdout, socket)." :parse-fn pint :default 200]
      ["-m" "--maxnum" "The max number of records to be generated in push-style server(e.g., stdout, socket). After that the server will stop pushing records" :parse-fn pint :default 5]
      ["-n" "--colnum" "How many columns in a row." :parse-fn pint :default 9]
      ["-l" "--colength" "How many characters can a column hold, e.g., the column size." :parse-fn pint :default 15]
      ["-f" "--fixedlen" "Should the size of every column be the same." :flag true :default true]
      ["-s" "--seperator" "What character will be the seperator for columns. This character will not be in characters seeds." :default "\t"]
      ["-w" "--words" "Use only lowcase alphabet character as seeds to generate random string. Otherwise all visiable characters will be used." :flag true :default true])))

(defn -main [& args]
  (let [[opts args banner] (pargs args)]
    (if (:help opts)
      (println banner)
      (let [svc (->> opts :type (str "svc-") symbol (ns-resolve 'datawell.main))]
        (svc opts)))))

