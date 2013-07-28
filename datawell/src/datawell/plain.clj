(ns datawell.gen.plain)

(defn- pint [x] (Integer. x))

(defn- gen-col
  "Compose a string with no more than *maxlen* long, by randomly picking characters from the *seeds*.
  If *fixed-len* is true, the strings generated every times will all be *maxlen* long, 
  otherwise their length are random. The *seeds* is a sequence of Integers corresponding to ASCII characters."
  [seeds maxlen fixed-len]
  (let [len (if fixed-len
    maxlen
    (inc (rand-int maxlen)))]
  (apply str (repeatedly len #(->> seeds (map char) rand-nth)))))

; (defn ex-args [mopts margs]
;   '(["-n" "--col-number" "The number of column in a record"
;     :default 5 :parse-fn pint]
;     ["-w" "--col-width" "The width in number of character of the column" 
;     :default 15 :parse-fn pint]
;     ["-f" "--[no-]fixed-len" "The length of every column is random within the limit of col-width"
;     :default false]
;     ["-a" "--alphabet" "If presented, the random string is generated only from alphabet(no special char e.g., + )"
;     :default false]))

(defn gen [idx pre {:keys [col-number col-width fixed-len alphabet]} args]
  (let [seeds (if alphabet
    (concat (range 65 91) (range 97 123))
    (range 32 127))]
  (repeatedly col-number #(gen-col seeds col-width fixed-len))))