(ns dev.client
  (:require [full.cljs.async :include-macros true]
            [replikativ.core :refer [client-peer wire]]
            [replikativ.p2p.fetch :refer [fetch]]
            [replikativ.p2p.block-detector :refer [block-detector]]
            [cljs.core.async :refer [chan pub sub]]
            [konserve.store :refer [new-mem-store]])
  (:require-macros [full.cljs.async :refer [go-try <? ]]
                   [cljs.core.async.macros :refer [go-loop]]))

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
   (>! out {:type :sub/identities
               :identities {"john" {42 #{"master"}}}
               :peer "STAGE"
               :id 43}))

  (go-try
   (dissoc (<? in) :id))


  (go-try
   (>! out {:type :connect/peer
               :url "ws://127.0.0.1:9090/"
               :peer "STAGE"
            :id 101}))

  (go-try
   (println (<? in)))
  
  )

