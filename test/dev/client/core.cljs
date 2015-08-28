(ns dev.client.core
  (:require [full.cljs.async :refer [throw-if-throwable] :include-macros true]
            [replikativ.core :refer [client-peer wire]]
            [replikativ.p2p.fetch :refer [fetch]]
            [replikativ.p2p.block-detector :refer [block-detector]]
            [cljs.core.async :refer [chan pub sub]]
            [konserve.store :refer [new-mem-store]])
  (:require-macros [full.cljs.async :refer [go-try <? ]]
                   [cljs.core.async.macros :refer [go-loop]]))

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

(.log js/console "DONE")
(comment


(start-local)

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
   (println (<? in)))
  
  )

