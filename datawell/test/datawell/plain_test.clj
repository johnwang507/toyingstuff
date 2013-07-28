(ns datawell.plain-test
  (:require [datawell.plain :refer :all])
  (:use midje.sweet))

(defn- all-alp? [x] (every? (->> (concat (range 65 91) (range 97 123)) (map char) set) x))

(facts "Generate a record"
  (let [arow (gen 0 0 {:col-number 9 :col-width 9 :fixed-len false :alphabet false} 0)
        lens (map count arow)]
    lens => (nine-of #(< % 10))
    (apply not= lens) => true
    (every? all-alp? arow) => false)
  (let [arow (gen 0 0 {:col-number 9 :col-width 9 :fixed-len true :alphabet true} 0)
        lens (map count arow)]
    lens => (nine-of 9)
    (apply not= arow) => true
    (every? all-alp? arow) => true))