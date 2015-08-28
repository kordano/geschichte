(ns dev.client.core
  (:require [weasel.repl :as repl]
            [konserve.store :refer [new-mem-store]]
            [full.cljs.async :refer [throw-if-throwable] :include-macros true]

            [replikativ.core :refer [client-peer wire]]
            [replikativ.p2p.fetch :refer [fetch]]
            [replikativ.p2p.block-detector :refer [block-detector]]
            [cljs.core.async :refer [chan pub sub]]
            )
  (:require-macros [full.cljs.async :refer [go-try <? ]]
                   [cljs.core.async.macros :refer [go-loop]]))

(repl/connect "ws://localhost:9001")

(.log js/console "Greetings from replikativ i/o")

(enable-console-print!)

(defn start-local []
  (go-try
   (let [_ (def local-store (<? (new-mem-store)))
         _ (def err-ch (chan))
         _ (def local-peer (client-peer "CLIENT" local-store err-ch
                                    (comp (partial block-detector :local)
                                          (partial fetch local-store err-ch))))
         _ (def in (chan))
         _ (def out (chan))]
     (<? (wire local-peer [out in])))))


(comment


  (start-local)
  
  (go-try
   (println (<? in)))

(go-try
   (>! out {:type :sub/identities
               :identities {"john" {42 #{"master"}}}
               :peer "STAGE"
            :id 43}))

  (go-try
   (println (dissoc (<? in) :id)))


  (go-try
   (>! out {:type :connect/peer
               :url "ws://127.0.0.1:9090/"
               :peer "STAGE"
            :id 101}))
  (go-try
   (>! out {:identities {"john" {42 #{"master"}}},
             :peer "CLIENT",
             :type :sub/identities-ack
             :id :ignored}))
  
  

  (go-try
   (>! out {:type :pub/downstream,
               :peer "STAGE",
               :id 1001
               :downstream {"john" {42 {:crdt :repo
                                        :op {:method :new-state
                                             :commit-graph {1 []
                                                            2 [1]}
                                             :branches {"master" #{2}}}
                                        :description "Bookmark collection."
                                        :public false}}}}))


  


  (go-try
   (>! out {:type :fetch/edn-ack
               :id 1001
               :values {1 {:transactions [[10 11]]}
                        2 {:transactions [[20 21]]}}}))


  (go-try
   (>! out {:type :fetch/edn-ack,
               :values {10 100
                        11 110
                        20 200
                        21 210}}))

  (go-try
   (>! out {:type :pub/downstream-ack
               :id 1001
            :peer "CLIENT"}))

  (go-try
   (>! out {:type :pub/downstream,
               :peer "STAGE",
               :id 1002
               :downstream {"john" {42 {:crdt :repo
                                        :op {:method :new-state
                                             :commit-graph {1 []
                                                            2 [1]
                                                            3 [2]}
                                             :branches {"master" #{3}}},
                                        :description "Bookmark collection.",
                                        :public false}}}}))

  
 (go-try
  (.log js/console (<? in)))
 
  )

