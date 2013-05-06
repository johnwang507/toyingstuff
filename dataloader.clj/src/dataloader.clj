(ns dataloader
  (:use [clojure.tools.cli :only [cli]]
        [clojure.string :only [split]])
  (:import [au.com.bytecode.opencsv.CSVReader]
           [java.io InputStreamReader FileInputStream]))

(defn comm-db [clz subp subn] {:classname clz :subprotocol subp :subname subn})

(def dbs {:sqlserver (fn [host port dbnm user pwd] 
                        {:classname "com.microsoft.jdbc.sqlserver.SQLServerDriver"
                         :subprotocol "sqlserver"
                         :subname (format "//%s:%s;database=%s;user=%s;password=%s" host port dbnm user pwd)})
          :oracle (fn [host port dbnm user pwd] 
                        {:classname "oracle.jdbc.driver.OracleDriver"
                         :subprotocol "oracle"
                         :subname (format "thin:@%s:%s:%s" host port dbnm)
                         :user user
                         :password pwd})
          :mysql (fn [host port dbnm user pwd] 
                        {:classname "com.mysql.jdbc.Driver"
                         :subprotocol "mysql"
                         :subname (format "//%s:%s/%s" host port dbnm)
                         :user user
                         :password pwd})
          :postgresql (fn [host port dbnm user pwd] 
                        {:classname "org.postgresql.Driver"
                         :subprotocol "postgresql"
                         :subname (format "//%s:%s/%s" host port dbnm)
                         :user user
                         :password pwd})
          :sqlite (fn [host port dbnm user pwd] 
                        {:classname "org.sqlite.JDBC"
                         :subprotocol "sqlite"
                         :subname dbnm}))
          
          
          
          :mysql :postgresql :sqlite})
           
(defn get-args [args]
  "Parse cmdline arguments into "
  (cli args
  ["-h" "--help" "Show help" :flag true :default false]
  ["-w" "--raw_date_fmt" "The date format in the data file" 
    :parse-fn #(re-find #"" %) :default "%m/%d/%Y %I:%M:%S %p"]))

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
        
(defn dbspec [opts]

{:classname "com.microsoft.jdbc.sqlserver.SQLServerDriver"
               :subprotocol "sqlserver"
               :subname "//db-host:port;database=db-name;user=user;password=password"  
  
  {:classname "oracle.jdbc.driver.OracleDriver"  ; must be in classpath
           :subprotocol "oracle"
           :subname "thin:@db-host:db-port:db-name" 
           :user "user"
           :password "pwd"}
  
{:classname "com.mysql.jdbc.Driver" ; must be in classpath
           :subprotocol "mysql"
           :subname (str "//" db-host ":" db-port "/" db-name)
           ; Any additional keys are passed to the driver
           ; as driver-specific properties.
           :user "a_user"
           :password "secret"}))
           
{:classname "org.postgresql.Driver" ; must be in classpath
           :subprotocol "postgresql"
           :subname (str "//" db-host ":" db-port "/" db-name)
           ; Any additional keys are passed to the driver
           ; as driver-specific properties.
           :user "a_user"
           :password "secret"}))

{:classname "org.sqlite.JDBC"
    :subprotocol "sqlite"
    :subname "db/database.db"})
    
    
(defn resolve [raw-data mapping]
  ())

(defn dump-data [table-data mapping]
  ())

(let [[opts args banner] (get-args *command-line-args*)]
  
  )          
