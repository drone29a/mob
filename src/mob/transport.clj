(ns mob.transport
  (:import (java.net DatagramPacket
                     InetAddress)))

;; :udp, :tcp, :hand-gestures
(defstruct connection-info
  :transport)

(defn make-packet
  "Make packet for some data."
  [#^String ip #^Integer port data]
  (let [data-bytes (.getBytes (str data) "UTF-8")]
    (DatagramPacket. data-bytes
                     (count data-bytes)
                     (InetAddress/getByName ip) 
                     port)))

(defn extract-message
  [#^DatagramPacket packet]
  (read-string (String. (.getData packet) "UTF-8")))

