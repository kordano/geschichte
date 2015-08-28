(ns dev.remote.core
  (:require [konserve.store :refer [new-mem-store]]
            [replikativ.p2p.fetch :refer [fetch]]
            [replikativ.platform-log :refer [warn info debug]]
            [replikativ.p2p.block-detector :refer [block-detector]]
            [replikativ.platform :refer [create-http-kit-handler! start stop]]
            [full.async :refer [<?? <?] :include-macros true]
            [clojure.core.async :refer [chan go-loop go]]
            [full.async :refer [go-try <?]]
            [replikativ.core :refer [server-peer wire]]))


(defn initialize-remote []
  (let 
      [handler (create-http-kit-handler! "ws://127.0.0.1:9090/")
       ;; remote server to sync to
       remote-store (<?? (new-mem-store))
       err-ch (chan)
       _ (go-loop [e (<? err-ch)]
           (when e
             (warn "ERROR:" e)
             (recur (<? err-ch))))
       _ (def remote-peer (server-peer handler "REMOTE"
                                       remote-store err-ch
                                       (comp (partial block-detector :remote)
                                             #_(partial logger log-atom :remote-core)
                                             (partial fetch remote-store err-ch))))]
    ))


(comment

  (initialize-remote)

  (start remote-peer)

  )
