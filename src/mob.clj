(ns mob
  (:require [clojure.contrib.logging :as log])
  (:use [clojure.contrib.seq-utils :only [shuffle]]
        [mob.core])
  (:import (java.net DatagramSocket 
                     DatagramPacket
                     InetSocketAddress
                     InetAddress)
           (java.util.concurrent Executors
                                 TimeUnit)))
(def listener-thread nil)
(def talker-task nil)

(defn start
  "Start the listener and talker."
  []
  (swap! stop? (fn [_] false))
  (swap! node-clock inc)
  (dosync 
   (alter members #(assoc % *name* {:status :up
                                      :connection {:transport :udp
                                                   :ip *ip*
                                                   :port *port*}})))
  (let [l (-> (Executors/defaultThreadFactory)
              (.newThread (bound-fn* listener)))
        t (-> (Executors/newScheduledThreadPool 5)
              (.scheduleAtFixedRate (bound-fn* talker) 0 *phi* TimeUnit/SECONDS))]
    (.start l)
    (log/info "started")
    (alter-var-root #'listener-thread (fn [_] l))
    (alter-var-root #'talker-task (fn [_] t))))

(defn stop
  "Stop the listener and talker."
  []
  (swap! stop? (fn [_] true))
  (.cancel talker-task))

(defn list-nodes
  []
  (keys @members))

(defn node-info
  [name]
  (@members (keyword name)))