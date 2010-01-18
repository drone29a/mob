(defproject mob "0.1.0-SNAPSHOT" 
  :description "A gossip library for creating and managing groups of Clojure nodes." 
  :dependencies [[org.clojure/clojure "1.1.0-master-SNAPSHOT"] 
                 [org.clojure/clojure-contrib "1.0-SNAPSHOT"]
                 [log4j "1.2.15" :exclusions [javax.mail/mail
                                              javax.jms/jms
                                              com.sun.jdmk/jmxtools
                                              com.sun.jmx/jmxri]]
                 [com.trottercashion/bert-clj "1.0"]]
  :dev-dependencies [[org.clojure/swank-clojure "1.0"]])
