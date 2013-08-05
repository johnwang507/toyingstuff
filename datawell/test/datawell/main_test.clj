(ns datawell.main-test
  (:require [datawell.main :as main])
  (:use midje.sweet))

(defn- all-alp? [x] (every? (->> (concat (range 65 91) (range 97 123)) (map char) set) x))

(facts "Generate a record"
  (let [arow (main/spout 0 0 {:colnum 9 :colsize 9 :fixedlen false :alphabet false} 0)
    lens (map count arow)]
    lens => (nine-of #(< % 10))
    (apply not= lens) => true
    (every? all-alp? arow) => false)
  (let [arow (main/spout 0 0 {:colnum 9 :colsize 9 :fixedlen true :alphabet true} 0)
    lens (map count arow)]
    lens => (nine-of 9)
    (apply not= arow) => true
    (every? all-alp? arow) => true))