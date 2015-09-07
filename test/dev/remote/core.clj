(ns dev.remote.core
  (:require [konserve.store :refer [new-mem-store]]
            [replikativ.p2p.fetch :refer [fetch]]
            [replikativ.platform-log :refer [warn info debug]]
            [replikativ.crdt.repo.realize :refer :all]
            [replikativ.p2p.block-detector :refer [block-detector]]
            [replikativ.platform :refer [create-http-kit-handler! start stop]]
            [replikativ.crdt.repo.stage :as s]
            [replikativ.stage :refer [create-stage! connect! subscribe-repos!]]
            [replikativ.crdt.repo.repo :as repo]
            [full.async :refer [<?? <? go-try] :include-macros true]
            [clojure.core.async :refer [chan go-loop go]]
            [replikativ.core :refer [server-peer wire]]))

(def uri "ws://127.0.0.1:31744")
(def repo-id #uuid "8e9074a1-e3b0-4c79-8765-b6537c7d0c44")

(def eval-fns
  {'(fn [old params] params) (fn [old params] params)
   '(fn [old params] (inc old)) (fn [old params] (inc old))
   '(fn [old params] (dec old)) (fn [old params] (dec old))
   '+ +})

(defn init []
  (let [err-ch (chan)
        handler (create-http-kit-handler! uri err-ch)
        remote-store (<?? (new-mem-store))
        _ (go-loop [e (<? err-ch)]
            (when e
              (warn "ERROR:" e)
              (recur (<? err-ch))))
        remote-peer (server-peer handler "REMOTE"
                                 remote-store err-ch
                                 (comp (partial block-detector :remote)
                                       (partial fetch remote-store err-ch)))
        stage (<?? (create-stage! "kordano@replikativ.io" remote-peer err-ch eval-fns))
        rp (<?? (s/create-repo! stage :description "testing" :id repo-id))
        state {:store remote-store
               :stage stage
               :repo rp
               :peer remote-peer}]
    (start remote-peer)
    state))


(comment

  (def remote-state (init))

  (clojure.pprint/pprint (:peer remote-state))
  
  (<?? (subscribe-repos! (:stage remote-state) {"kordano@replikativ.io" {repo-id #{"master"}}}))
  
  (<?? (s/transact (:stage remote-state)
                   ["kordano@replikativ.io" repo-id "master"]
                   '(fn [old params] (inc old))
                   42
                   ))

  (<?? (s/commit! (:stage remote-state) {"kordano@replikativ.io" {repo-id #{"master"}}}))
  
  
  
  (-> remote-state :store :state deref clojure.pprint/pprint)

  (-> remote-state :store :state deref (get ["kordano@replikativ.io" repo-id]) :state :commit-graph)

  
  
  
  )
