(def pint #(Integer. %))

(defn ex-args []
	'(["-n" "--col_number" "The number of column in a record"
			:default 5 :parse-fn pint]
		["-w" "--col_width" "The width in number of character of the column" 
			:default 15 :parse-fn pint]
		["-f" "--[no-]fixed_len" "The length of every column is random within the limit of col_width"
			:default false]
		["-a" "--alphabet" "If presented, the random string is generated only from alphabet(no special char e.g., + )"
			:default false]))

(defn gen-col [seeds maxlen fixed_len]
	(let [len (if fixed_len
								maxlen
								(inc (rand-int maxlen)))]
		(apply str (repeatedly len #(->> seeds (map char) rand-nth)))))

(defn gen [idx pre {:keys [col_number col_width fixed_len alphabet]} args]
	(let [seeds (if alphabet
									(concat (range 65 91) (range 97 123))
									(range 32 127))]
		(repeatedly col_number #(gen-col seeds col_width fixed_len))))



defn gen-col seeds maxlen fixed_len
	let len if fixed_len
						 maxlen
						 inc 
						 	 rand-int maxlen
		apply str repeatedly len 
		                     #(->> seeds
													     map char
													     rand-nth)

defn gen idx pre {:keys [col_number col_width fixed_len alphabet]} args
	let seeds if alphabet
					     concat 
					     	 range 65 91
					     	 range 97 123
					     range 32 127
			foo 1
		repeatedly col_number #(gen-col seeds col_width fixed_len)