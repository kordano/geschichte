(ns dev.remote
  (:require [konserve.store :refer [new-mem-store]]
            [replikativ.p2p.block-detector :refer [block-detector]]
            [full.cljs.async :include-macros true]
            [clojure.core.async :refer [chan]]
            [full.async :refer [go-try <?]]
            [replikativ.core :refer [client-peer wire]]))


(defn run-it []
  (go-try
   (let [local-store (<? (new-mem-store))
         err-ch (chan)
         _ (println "1")
         local-peer (client-peer "CLIENT" local-store err-ch
                                 (partial block-detector :local))
         _ (println "2")
         in (chan)
         out (chan)
         ]
     (<? (wire local-peer [out in])))))
