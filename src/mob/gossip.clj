(ns mob.core
  (:require [clojure.contrib.logging :as log])
  (:use [clojure.contrib.seq-utils :only [shuffle]]
        [mob.transport])
  (:import (java.net DatagramSocket 
                     DatagramPacket
                     InetSocketAddress
                     InetAddress)
           (java.util.concurrent Executors
                                 TimeUnit)))

(def *name* (keyword (str "node" (rand-int 1000000))))
(def *ip* (-> (InetAddress/getLocalHost)
              .getHostAddress))
(def *port* 1108)

;; Number of nodes to gossip with at once
(def *gossip-num* 1)

;; Rate of active gossip in seconds
(def *phi* 10)

;; Used to keep time and mark vector clocks,
;; this clock is for this node.
(def node-clock (atom 0))

;; List of known mob members and their status
;; {:node-name {:status :up
;;              :connection {:transport :udp
;;                           :ip ip-addr
;;                           :port port-num}}}
(def members (ref {}))

;; Flag to stop listening
(def stop? (atom false))

(defstruct message 
  :sender
  :body)

(defn update-members
  "Update the membership list by merging and inferring 
from another node's membership knowledge."
  [other-members]
  (dosync
   (alter members #(merge % other-members))))

(defn message-handler
  "Handles incoming messages."
  [msg]
  (swap! node-clock inc)
  (let [sender (:sender msg)
        other-members (:body msg)]
    (update-members other-members)))

(defn random-nodes
  "Returns the names of n random nodes."
  [n]
  (take n (shuffle (clojure.set/difference (set (keys @members))
                                           #{*name*}))))

(defmulti gossip "Send juicy gossip to a node." :transport)
(defmethod gossip :udp
  [conn-info]
  (let [socket (DatagramSocket. 0 (InetAddress/getByName *ip*))
        packet (make-packet (:ip conn-info) 
                            (:port conn-info)
                            (struct message *name*
                                            @members))]
    (.connect socket (InetAddress/getByName (:ip conn-info)) (:port conn-info))
    (.send socket packet)
    ;; TODO: is this wasteful use of sockets?
    (.close socket)
    (log/debug (str "gossip sent to " (:ip conn-info) ":" (:port conn-info)))))

(defn talker
  "Gossip to some nodes."
  []
  (let [nodes (random-nodes *gossip-num*)]
    (doseq [n nodes]
      (log/debug (str "talking to " n))
      (gossip (:connection (@members n))))))

(defn listener 
  "Listens for incoming messages and launches handlers.

This function should run in its own thread on every node."
  []
  (let [socket (DatagramSocket. (InetSocketAddress. *ip* *port*))
        packet (DatagramPacket. (byte-array 65536) 65536)]
    (loop []
      (.receive socket packet)
      (log/debug (str "packet received from " (.getSocketAddress packet)))
      (send-off (agent (extract-message packet)) 
                (bound-fn* message-handler))
      (if @stop?
        nil
        (recur)))
    (.close socket)))