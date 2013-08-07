(ns datawell.main
  (:use [clojure.tools.cli :only [cli]]
        [clojure.java.io :only [file]]
        [clojure.string :only [join]]
        [ring.adapter.jetty :only [run-jetty]]
        [ring.util.response :only [response]])
  (:gen-class))

(def ^:private roof (atom -1))
(def ^:private counter (atom 0))

(defn- gen-col
  "Compose a string with no more than *maxlen* long, by randomly picking characters from the *seeds*.
  If *fixedlen* is true, the strings generated every times will all be *maxlen* long, 
  otherwise their length are random. The *seeds* is a sequence of Integers corresponding to ASCII characters."
  [seeds maxlen fixedlen]
  (let [len (if fixedlen maxlen (inc (rand-int maxlen)))]
  (apply str (repeatedly len #(->> seeds (map char) rand-nth)))))

(defn spout [{:keys [colnum colsize fixedlen alphabet limit]}]
  (let [seeds (if alphabet (concat (range 65 91) (range 97 123)) (range 32 127))]
      (repeatedly colnum #(gen-col seeds colsize fixedlen))))

(defn limitspout [f]
  (if (or (< roof 0) (< counter roof))
      (do (swap! counter inc)
          (f))
      (System/exit 0)))

(defn svc-http [opts args]
    (run-jetty (fn [request] (limitspout #(response (spout opts)))) {:port (:port opts)}))

(defn svc-stdout [opts args] (limitspout #(println (spout opts))))

(defn svc-socket [opts args]
  (run-socket handler {:port (:port opts)})) ;go here 

(defn svc-zeromq [opts args] 1);TODO 

(def ifc-types [1 #'svc-stdout 2 #'svc-http 3 #'svc-socket 4 #'svc-zeromq])

(defn- pargs [args]
  (let [pint #(Long. %)
    type-opts (->> ifc-types (take-nth 2) (join ", "))
    type-nms (->> ifc-types rest (take-nth 2) (map #(-> % meta :name name (.substring 4))) (join ", "))]
  (cli args
    ["-h" "--help" "Show this help." :flag true :default false]
    ["-t" "--type" (str "The type of the service. Avaliable: " type-opts ", standing for " type-nms ".") :parse-fn pint :default 1]
    ["-p" "--port" "The port to bind" :parse-fn pint :default 9876]
    ["-i" "--interval" "The time interval between two consecutive records being generated, in millisecond. Ignored if the type of service is pull-style(e.g., http) instead of push-style(e.g., stdout, socket)." :parse-fn pint :default 200]
    ["-l" "--limit" "How many records to be generated before the service stop. a negative value makes the generating never stop." :parse-fn pint :default 5]
    ["-n" "--colnum" "How many columns in a row." :parse-fn pint :default 9]
    ["-s" "--colsize" "How many characters can a column hold, e.g., the column size." :parse-fn pint :default 15]
    ["-f" "--fixedlen" "Should the size of every column be the same." :flag true :default true]
    ["-a" "--allchars" "Use all visiable characters to generate data. Otherwise only alphabet will be used." :flag true :default true])))

(defn -main [& args]
  (let [[opts args banner] (pargs args)]
    (when (:help opts)
      (println banner)
      (System/exit 0))
    (let [svc (->> opts :type (.indexOf ifc-types) inc (nth ifc-types))]
      (reset! roof (:limit opts))
      (meta svc))))

