(ns dev.client.core
  (:require [weasel.repl :as repl]
            [konserve.store :refer [new-mem-store]]
            [konserve.protocols :refer [-get-in]]
            [replikativ.core :refer [wire client-peer]]
            [replikativ.stage :refer [create-stage! connect! subscribe-repos!]]
            [replikativ.p2p.fetch :refer [fetch]]
            [replikativ.p2p.hash :refer [ensure-hash]]
            [replikativ.p2p.log :refer [logger]]
            [replikativ.p2p.hooks :refer [hook]]
            [replikativ.crdt.repo.stage :as s]
            [replikativ.p2p.block-detector :refer [block-detector]]
            [cljs.core.async :refer [>! chan]]
            [full.cljs.async :refer [throw-if-throwable]])
  (:require-macros [full.cljs.async :refer [go-try <? go-loop-try]]
                   [cljs.core.async.macros :refer [go-loop]]))

(repl/connect "ws://localhost:9001")

(def repo-id #uuid "8e9074a1-e3b0-4c79-8765-b6537c7d0c44")

(.log js/console "Greetings from replikativ i/o")

(def uri "ws://127.0.0.1:31744")

(def hooks (atom {[#".*"
                   repo-id
                    "master"]
                  [["kordano@replikativ.io"
                    repo-id
                    "master"]]}))

(enable-console-print!)

(def eval-fns
  {'(fn [old params] params) (fn [old params] params)
   '(fn [old params] (inc old)) (fn [old params] (inc old))
   '(fn [old params] (dec old)) (fn [old params] (dec old))
   '+ +})

(defn start-local []
  (go-try
   (let [local-store (<? (new-mem-store))
         err-ch (chan)
         log-atom (atom {})
         local-peer (client-peer "CLIENT" local-store err-ch
                                 (comp (partial logger log-atom :local-core)
                                       (partial hook hooks local-store)
                                       (partial fetch local-store err-ch)))
         stage (<? (create-stage! "kordano@replikativ.io" local-peer err-ch eval-fns))
         _ (go-loop [e (<? err-ch)]
            (when e
              (.log js/console "ERROR:" e)
              (recur (<? err-ch))))]
     {:store local-store
      :stage stage
      :log log-atom
      :error-chan err-ch
      :peer local-peer})))


(comment
  

  (go-try (def client-state (<? (start-local))))
  
  (go-try (<? (subscribe-repos! (:stage client-state) {"kordano@replikativ.io" {repo-id #{"master"}}})))
  
  (go-try (<? (connect! (:stage client-state) uri)))
  
  (println (-> client-state :stage deref :config))


  (println (-> client-state :log deref))
  
  (-> client-state :store :state deref (get ["kordano@replikativ.io" repo-id]) :state :commit-graph)

  
  )  
