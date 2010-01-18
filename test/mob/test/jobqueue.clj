(ns mob.test.jobqueue
  (:require [mob.jobqueue :as jobqueue]
            [mob.job :as job])
  (:use [clojure.test]))

(def *jq* nil)

(defn jq-fixture
  [f]
  (binding [*jq* (jobqueue/create :test-jq)]
    (doto *jq*
      (jobqueue/put (job/create "frothy"))
      (jobqueue/put (job/create "or"))
      (jobqueue/put (job/create "not")))
    (f)))

(use-fixtures :each jq-fixture)

(deftest create
  (is (= :stuff (:name (jobqueue/create :stuff)))))

(deftest put
  (jobqueue/put *jq* (job/create 0 "another"))
  (is (= "another" (:data (jobqueue/reserve *jq*)))))

(deftest reserve
  (is (= "frothy" (:data (jobqueue/reserve *jq*))))
  (is (= "or" (:data (jobqueue/reserve *jq*))))
  (is (= "not" (:data (jobqueue/reserve *jq*)))))

(deftest bury
  (let [j (jobqueue/reserve *jq*)]
    (jobqueue/bury *jq* (:id j))
    (is (= (:state (jobqueue/stats *jq* (:id j))) :buried))))

(deftest delete
  (let [j (jobqueue/reserve *jq*)]
    (jobqueue/delete *jq* (:id j))
    (is (= (jobqueue/stats *jq* (:id j)) {}))))

(deftest stats
  (let [j (jobqueue/reserve *jq*)]
    (is (= (:id j) (:id (jobqueue/stats *jq* (:id j)))))
    (is (= (:state j) (:state (jobqueue/stats *jq* (:id j)))))))