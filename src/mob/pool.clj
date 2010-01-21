(ns mob.pool
  (:use [mob.jobqueue :only [reserve delete bury]])
  (:import (java.util.concurrent LinkedBlockingQueue
                                 ThreadPoolExecutor
                                 TimeUnit
                                 Executors
                                 ThreadFactory)))

(declare work-loop
         halt
         halt?
         active-count
         create-thread-factory)

(defn create
  "Create a worker pool.
  
  The pool will use `num-workers` number of workers to call
  the `work-fn` on jobs reserved from an attached jobqueue."
  [name num-workers work-fn jq]
  (let [tg (ThreadGroup. (str name))]
    {:name name
     :num-workers num-workers
     :work-fn work-fn
     :jobqueue jq
     :halt-flag (ref false)
     :executor (Executors/newFixedThreadPool num-workers
                                             (create-thread-factory tg))
     :thread-group tg})) ; TODO: use gensym to make unique?

(defn start
  "Begin processing jobs.

  Returns immediately if no jobqueue has been provided."
  [p]
  (when (:jobqueue p)
    (dotimes [_ (:num-workers p)]
      (.execute (:executor p) #(work-loop p))))
  ; TODO: throw exception if no jobqueue?
  )

(defn work-loop
  "Wrap the work fn with a fn that will reserve
  jobs, pass job data to the work fn, handle any exceptions,
  and then repeat."
  [p]
  (let [jq (:jobqueue p)
        f (:work-fn p)]
    (loop [j (reserve jq)]
      (when-not (halt? p)
        (do (try 
             (f (:data j))
             ;; Let function handle job deletion (or other things)
             (delete jq (:id j))
             (catch Exception e
               ; TODO: do something useful, user-provided error handler?
               ))
            (when-not (halt? p) 
              (recur (reserve jq))))))))

(defn halt
  "Halt a pool.  No further jobs will be reserved."
  [p]
  (dosync
   (ref-set (:halt-flag p) true))
  (.shutdown (:executor p))
  ; Interrupt threads?
  )

(defn halt?
  "Returns true if worker pool halted or is in process of halting."
  [p]
  @(:halt-flag p))

(defn active-count
  "Returns number of active workers."
  [p]
  (.getActiveCount (:executor p)))

(defn- create-thread-factory
  [p]
  (proxy [ThreadFactory] []
    (newThread [#^Runnable r]
               (Thread. (:thread-group p) r))))