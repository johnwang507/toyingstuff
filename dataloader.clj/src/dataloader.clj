(ns dataloader
  (:use [clojure.tools.cli :only [cli]]
        [clojure.string :only [split]])
  (:import [au.com.bytecode.opencsv.CSVReader]
           [java.io InputStreamReader FileInputStream]))

(defn dbs [{:keys [dbtype host port dbnm user pwd]}]
  (dbtype
    {:sqlserver {:classname "com.microsoft.jdbc.sqlserver.SQLServerDriver"
                :subprotocol "sqlserver"
                :subname (format "//%s:%s;database=%s;user=%s;password=%s" host port dbnm user pwd)}
    :oracle {:classname "oracle.jdbc.driver.OracleDriver"
             :subprotocol "oracle"
             :subname (format "thin:@%s:%s:%s" host port dbnm)
             :user user
             :password pwd}
    :mysql {:classname "com.mysql.jdbc.Driver"
            :subprotocol "mysql"
            :subname (format "//%s:%s/%s" host port dbnm)
            :user user
            :password pwd}
    :postgresql {:classname "org.postgresql.Driver"
                 :subprotocol "postgresql"
                 :subname (format "//%s:%s/%s" host port dbnm)
                 :user user
                 :password pwd}
    :sqlite {:classname "org.sqlite.JDBC"
             :subprotocol "sqlite"
             :subname dbnm}}))
           
(defn get-args [args]
  "Parse cmdline arguments into "
  (cli args
  ["-h" "--help" "Show help" :flag true :default false]
  ["-w" "--raw_date_fmt" "The date format in the data file" 
    :parse-fn #(re-find #"" %) :default "%m/%d/%Y %I:%M:%S %p"]))) ;TODO: complete

(defn fnm-kw [path] (-> path (split #"[/\\]") last (split #"\.") first keyword))
    
(defn parse-mp [rt [f1 f2 f3 f4 f5 f6 f7]]
            (let [[t-nm tf-nm tf-tp s-nm sf-nm] (map keyword [f1 f2 f3 f6 f7])
                  tf-dft f4 vmp f5]
            (if (or (seq rt) (and (seq t-nm) (-> rt last first (= t-nm))))
              (conj rt [(keyword t-nm), (keyword s-nm), [(keyword tf-nm), (keyword tf-tp),tf-dft,vmp,(keyword sf-nm)]])
              (conj (-> rt drop-last vec) (conj (last rt) [tf-nm, tf-tp,tf-dft,vmp,sf-nm])))))

(defn read-csvf [path encoding] (-> path FileInputStream. (InputStreamReader. encoding) CSVReader. .readAll))

(defn load-mapping [mfile opts]
  "Parse the mapping configure. Return a vector with following structure:
    [ 
      [table_name_1,source_csv_1 
        [field_name_1, field_type, default_value, vmap, source_field]
          ...
      ],
      ... ...
    ]"
  (reduce parse-mp [] (read-csvf mfile (:enc-cf opts))))
  
(defn used-sfields [snm mp] (->> (filter #(= snm (nth % 1)) mp)
                                  first rest rest
                                  (map #(-> % last keyword))
                                  set))

(defn load-csv [path mp opts]
  (let [snm (fnm-kw path)
        usfs (used-sfields snm mp)
        data (read-csvf path (:enc-data opts))
        sfs (->> data first (map #(keyword %)))
        srws (->> data rest (map #(list %1 %2) sfs))]
    (map (fn [idx rw] (->> rw (filter (fn [[k _]] (sfs k))) vec (conj [:rwid idx]))) (range) srws)))
        
    
(defn resolve [raw-data mapping]
  ())

(defn dump-data [table-data mapping]
  ())

(let [[opts args banner] (get-args *command-line-args*)]
  
  )          
