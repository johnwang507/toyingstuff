(ns datawell.plain-test
  (:require [datawell.plain :refer :all])
  (:use midje.sweet))

(def allseeds (concat (range 65 91) (range 97 123)))

(facts "About randomly generate columns"
  (fact "The max length argument works"
    (gen-col allseeds )))