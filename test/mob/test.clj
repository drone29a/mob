(ns mob.test
  (:use [clojure.test]))

(comment (def tests '(jobqueue))

         (def tests-ns (for [test tests] (symbol (str "mob.test." test)))))



