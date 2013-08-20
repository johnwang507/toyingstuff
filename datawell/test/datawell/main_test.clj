(ns datawell.main-test
  (:require [datawell.main :as main])
  (:use midje.sweet
        [clojure.string :only [split]]))

(defn- alllowcase? [x] (every? (->> (range 97 123) (map char) set) x))

(facts "Generate a record"
  (let [opts {:colnum 9 :colength 9 :fixedlen false :words false :seperator "\t"}
    col-seq (-> opts main/gen-row (split #"\t"))
    lens (map count col-seq)]
    lens => (nine-of #(< % 10))
    (apply not= lens) => true
    (every? alllowcase? col-seq) => false)
  (let [opts {:colnum 9 :colength 9 :fixedlen true :words true :seperator "\t"}
    col-seq (-> opts main/gen-row (split #"\t"))
    lens (map count col-seq)]
    lens => (nine-of 9)
    (apply not= col-seq) => true
    (every? alllowcase? col-seq) => true))