(ns mob.pool)

(defn create
  "Create a worker pool."
  [name num-workers]
  {:name name
   :num-workers num-workers})

(defn work-loop
  "Wrap the work fn with a fn that will reserve
  jobs, pass job data to the work fn, handle any exceptions,
  and then repeat."
  [jq f]
  (let [j (reserve jq)]
    (try 
     (f (:data j))
     (bury jq (:id j)))
    (delete jq (:id j))
    (recur jq f)))

(defn attach
  "Attach pool to a jobqueue.  Processing beings immediately."
  [p jq]
  (assoc p :jobqueue jq))

(comment
  (defn halt
    "Halt a pool.  No further jobs will be reserved."
    [p]))