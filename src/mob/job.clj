(ns mob.job)

(def default-priority 1000)

(def id-counter (ref 0))

(defn get-id
  "Increment id-counter and return it's original value."
  []
  (dosync 
   (let [id @id-counter]
     (alter id-counter inc)
     id)))

(defn create
  ([data]
     (create (get-id) default-priority data))
  ([pri data]
     (create (get-id) pri data))
  ([id pri data]
     {:id id
      :pri pri
      :data data}))