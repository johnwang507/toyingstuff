(ns dataloader
  (:use [clojure.tools.cli :only [cli]]))
  

(defn get-args [args]
  "Parse cmdline arguments into "
  (cli args
  ["-h" "--help" "Show help" :flag true :default false]
  ["-w" "--raw_date_fmt" "The date format in the data file" 
    :parse-fn #(re-find #"" %) :default "%m/%d/%Y %I:%M:%S %p"]))

(defn load-mapping [mfile]
  "Parse the mapping configure. Return a vector with following structure:
    [ 
      [table_name_1,source_csv_1
        [field_name_1, [field_type, default_value, vmap, source_field]]
        [field_name_2, [field_type, default_value, vmap, source_field]]
          ...
        ],
        ... ...
    ]"
  ())

(defn load-csv [fpath mapping]
  ())

(defn resolve [raw-data mapping]
  ())

(defn dump-data [table-data mapping]
  ())

(let [[opts args banner] (get-args *command-line-args*)]
  
  )          
