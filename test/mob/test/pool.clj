(ns mob.test.pool
  (:require [mob.pool :as pool]
            [mob.jobqueue :as jobqueue]
            [mob.job :as job])
  (:use [clojure.test]))

(def *jq* nil)
(def *pool* nil)
(def *results* (ref {}))

(defn jq-fixture
  [f]
  (binding [*jq* (jobqueue/create :test-jq)]
    (doto *jq*
      (jobqueue/put (job/create "frothy"))
      (jobqueue/put (job/create "or"))
      (jobqueue/put (job/create "not")))
    (f)))

(defn pool-fixture
  [f]
  (binding [*pool* (pool/create :test-pool 
                                5
                                (fn [d]
                                  ;; Add 1 and store in results map
                                  (dosync 
                                   (alter *results* assoc d (inc d))))
                                *jq*)]
    (f)))

(use-fixtures :each jq-fixture pool-fixture)

(deftest create
  (is (= :test-pool (:name *pool*)))
  (is (= 5 (:num-workers *pool*)))
  (is (= *jq* (:jobqueue *pool*))))

(deftest job-results
  (let [xs (range 10)]
    (doseq [x xs]
      (jobqueue/put *jq* (job/create x)))
    (pool/start *pool*)
    (Thread/sleep 3000)
    (doseq [x xs]
      (is (= (inc x) (get @*results* x))))))

(deftest halt?
  (is (= false (pool/halt? *pool*)))
  (pool/halt *pool*)
  (is (= true (pool/halt? *pool*))))

(deftest active-count
  (is (= 0 (pool/active-count *pool*)))
  (doseq [x (range 100000)]
    (jobqueue/put *jq* (job/create x)))
  (pool/start *pool*)
  (is (> (pool/active-count *pool*) 0)))