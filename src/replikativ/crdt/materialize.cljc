(ns replikativ.crdt.materialize
  (:require [konserve.protocols :refer [-get-in -assoc-in -bget -bassoc]]
            [replikativ.crdt :refer [map->Repository]]
            #?(:clj [replikativ.crdt.repo.impl :refer :all]
               :cljs [replikativ.crdt.repo.impl])
            #?(:clj [full.async :refer [<? go-try]])
            #?(:cljs [full.cljs.async :refer [throw-if-throwable]])
            #?(:clj [clojure.core.async :as async
                     :refer [>! timeout chan alt! go put! go-loop sub unsub pub close!]]
               :cljs [cljs.core.async :as async
                      :refer [>! timeout chan put! sub unsub pub close!]]))
  #?(:cljs (:require-macros [full.cljs.async :refer [<? go-try]])))


;; make extendable? multimethod?
(defn pub->crdt
  ([crdt-type]
   (go-try (case crdt-type
             :repo (map->Repository {}))))
  ([store [user repo] crdt-type]
   (go-try (case crdt-type
             :repo
             (map->Repository (assoc (<? (-get-in store [[user repo] :state]))
                                     :cursor [[user repo] :state]
                                     :store store))

             (throw (ex-info "Cannot materialize CRDT for publication."
                             {:user user
                              :repo repo
                              :crdt-type crdt-type}))))))
