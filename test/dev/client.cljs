(ns dev.client
  (:require [konserve.store :refer [new-mem-store]]
            [replikativ.p2p.block-detector :refer [block-detector]]
            [full.cljs.async :include-macros true]
            [cljs.core.async :refer [chan pub sub]]
            [replikativ.core :refer [client-peer wire]])
  (:require-macros [full.cljs.async :refer [go-try <? ]]
                   [cljs.core.async.macros :refer [go-loop]]))

(enable-console-print!)

(println "Hello replikativ")

(defn run-it []
  (go-try
   (let [_ (println "1")
         local-peer :foo #_(client-peer "CLIENT" :local :err :block-detector)
         _ (println "2")
         in (chan)
         out (chan)]
     (println local-peer)
     )))

(run-it)

