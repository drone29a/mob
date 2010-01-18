(ns mob.jobqueue
  (:import (java.util.concurrent PriorityBlockingQueue 
                                 TimeUnit)))
(declare a-queue
         get-job
         add-job
         remove-job)

(defn create
  "Make a jobqueue.
  
  A jobqueue has a few components: a ready queue holding jobs to be processed,
  a map of buried jobs, and a map of reserved jobs."
  [name]
  (let [jq {:name name
            :jobs (ref {})}]
    (-> jq
        (assoc :ready-q (a-queue (:jobs jq)))
        (assoc :buried-q (a-queue (:jobs jq))))))

(defn put
  "Put a job into the jobqueue."
  [jq j]
  (let [j-ready (-> j 
                    (assoc :state :ready)
                    (assoc :timestamp (System/currentTimeMillis)))]
    (add-job jq j-ready)
    (.put (:ready-q jq) (:id j-ready))))

(defn reserve
  "Reserve a job."
  ([jq]
     ;; Blocking for a job is default behavior.
     (let [j (-> (get-job jq (.take (:ready-q jq))) 
                 (assoc :state :reserved))]
       (add-job jq j)
       j))
  ([jq timeout-secs]
     (let [j (-> (get-job jq (.poll (:ready-q jq) timeout-secs TimeUnit/SECONDS))
                 (assoc :state :reserved))]
       (add-job jq j)
       j)))

(defn bury
  "Bury a job."
  [jq j-id]
  ;; TODO: support priorities and 
  ;; if already in buried-q, only updated if priority changed.
  (dosync
   (let [j-orig (get-job jq j-id)]
     (when (= (:state j-orig) :ready)
       (alter (:ready-q jq) dissoc j-id))
     (add-job jq (assoc j-orig :state :buried))
     (.put (:buried-q jq) j-id))))

(defn kick
  "Kick a bounded number of jobs from the buried queue to the ready queue.

  Returns the number of jobs kicked."
  ([jq]
     (kick jq 0))
  ([jq bound]
     (let [bq (:buried-q jq)
           bound (Math/min bound (.size bq))
           kicked-ids (filter #(not (nil? %)) 
                              (take bound (repeatedly #(.poll bq))))]
       (doseq [k-id kicked-ids]
         (put jq (get-job jq k-id)))
       (count kicked-ids))))

(defn delete
  "Delete a job.  Usually called after successful completion."
  [jq j-id]
  (condp :state (get-job jq j-id)
    :ready (.remove (:ready-q jq) j-id)
    :buried (.remove (:buried-q jq) j-id))
  (remove-job jq j-id))

(defn stats
  "Provide statistics of the jobqueue or a given job in the jobqueue."
  ([jq j-id]
     (select-keys (get-job jq j-id) [:id :state]))
  ([jq]
     {}))

(defn- get-job
  [jq j-id]
  (@(:jobs jq) j-id))

(defn- add-job
  "Index jobs by id in the jobs map."
  [jq j]
  (dosync 
   (alter (:jobs jq) assoc (:id j) j)))

(defn- remove-job
  [jq j-id]
  (dosync
   (alter (:jobs jq) dissoc j-id)))

(defn- a-queue
  [jobs]
  (PriorityBlockingQueue. 32 (proxy [java.util.Comparator] [] 
                                         (compare [x-id y-id]
                                                  (let [x (@jobs x-id)
                                                        y (@jobs y-id)
                                                        x-pri (:pri x)
                                                        y-pri (:pri y)
                                                        x-timestamp (:timestamp x)
                                                        y-timestamp (:timestamp y)]
                                                    (cond (> x-pri y-pri) 1
                                                          (< x-pri y-pri) -1
                                                          :else (cond (> x-id y-id) 1
                                                                      (< x-id y-id) -1
                                                                      :else 0)))))))
