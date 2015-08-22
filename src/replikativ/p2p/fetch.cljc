(ns replikativ.p2p.fetch
  "Fetching middleware for replikativ. This middleware covers the exchange of the actual content (commits and transactions, not metadata) of repositories."
  (:require [replikativ.environ :refer [store-blob-trans-id]]
            [replikativ.protocols :refer [-missing-commits -downstream]]
            [replikativ.platform-log :refer [debug info warn error]]
            [replikativ.crdt.materialize :refer [pub->crdt]]
            [full.async :refer [<? <<? go-try go-for go-loop-try go-loop-try>]]
            [konserve.protocols :refer [-assoc-in -exists? -get-in -update-in
                                        -bget -bassoc]]
            [clojure.set :as set]
            #?(:clj [clojure.java.io :as io])
            #?(:clj [clojure.core.async :as async
                      :refer [>! timeout chan alt! go put! pub sub unsub close!]]
               :cljs [cljs.core.async :as async
                      :refer [>! timeout chan put! pub sub unsub close!]]))
  #?(:clj (:import [java.io ByteArrayOutputStream])))



(defn- not-in-store?! [store transactions pred]
  (->> (go-for [tx transactions
                :when (pred (first tx))
                id tx
                :when (not (<? (-exists? store id)))]
               id)
       (async/into #{})))


(defn- new-transactions! [store transactions]
  (not-in-store?! store transactions #(not= % store-blob-trans-id)))


(defn- new-blobs! [store transactions]
  (go-try (->> (not-in-store?! store transactions #(= % store-blob-trans-id))
               <?
               (filter #(not= % store-blob-trans-id)))))


(defn fetch-commit-values!
  "Resolves all commits recursively for all nested CRDTs. Starts with commits in pub."
  [out fetched-ch store [user repo] pub pub-id]
  (go-try (let [crdt (<? (pub->crdt store [user repo] (:crdt pub)))
                crdt (-downstream crdt (:op pub))] ;; TODO should be unnecessary
            (loop [ncs (<? (-missing-commits crdt out fetched-ch (:op pub)))
                   cvs {}]
              (if (empty? ncs) cvs
                  (do
                    (info "starting to fetch " ncs "for" pub-id)
                    (>! out {:type :fetch/edn
                             :id pub-id
                             :ids ncs})
                    (let [ncvs (merge cvs (select-keys (:values (<? fetched-ch)) ncs))
                          ncs  (->> (go-for [crdt (mapcat :crdt-refs (vals ncvs))]
                                            (let [nc (<? (-missing-commits (assoc crdt :store store)
                                                                           out fetched-ch))]
                                              nc))
                                    <<?
                                    (apply set/union))]
                      (recur (set (filter (comp not ncvs) ncs)) ;; break crdt recursion
                             ncvs))))))))


;; TODO don't fetch too huge blocks at once, slice
(defn fetch-and-store-txs-values! [out fetched-ch store txs pub-id]
  (go-try (let [ntc (<? (new-transactions! store txs))]
            ;; transactions first
            (when-not (empty? ntc)
              (debug "fetching new transactions" ntc "for" pub-id)
              (>! out {:type :fetch/edn
                       :id pub-id
                       :ids ntc})
              (if-let [tvs (select-keys (:values (<? fetched-ch)) ntc)]
                (doseq [[id val] tvs]
                  (debug "trans assoc-in" id (pr-str val))
                  (<? (-assoc-in store [id] val))))))))


(defn fetch-and-store-txs-blobs! [out binary-fetched-ch store txs pub-id]
  (go-try (let [nblbs (<? (new-blobs! store txs))]
            (when-not (empty? nblbs)
              (debug "fetching new blobs" nblbs "for" pub-id)
              (<? (go-loop-try [[to-fetch & r] nblbs]
                               (when to-fetch
                                 ;; recheck store to avoid double fetching of large blobs
                                 (if (<? (-exists? store to-fetch))
                                   (recur r)
                                   (do
                                     (>! out {:type :fetch/binary
                                              :id pub-id
                                              :blob-id to-fetch})
                                     (let [{:keys [value]} (<? binary-fetched-ch)]
                                       (debug "blob assoc" to-fetch)
                                       (<? (-bassoc store to-fetch value))
                                       (recur r)))))))))))


(defn store-commits! [store cvs]
  (go-try (<<? (go-for [[k v] cvs]
                       (<? (-assoc-in store [k] v))))))

(defn- fetch-new-pub
  "Fetch all external references."
  [store err-ch p pub-ch [in out]]
  (let [fetched-ch (chan)
        binary-fetched-ch (chan)
        all-true? (fn [x] (if (seq? x) (reduce #(and %1 %2)) x))]
    (sub p :fetch/edn-ack fetched-ch)
    (sub p :fetch/binary-ack binary-fetched-ch)
    ;; TODO err-channel
    (go-loop-try> err-ch [{:keys [type downstream values peer] :as m} (<? pub-ch)]
      (when m
        ;; TODO abort complete update on error gracefully
        (<<? (go-for [[user repos] downstream
                      [repo pub] repos]
                     (let [cvs (<? (fetch-commit-values! out fetched-ch store [user repo] pub (:id m)))
                           txs (mapcat :transactions (vals cvs))]
                       (<? (fetch-and-store-txs-values! out fetched-ch store txs (:id m)))
                       (<? (fetch-and-store-txs-blobs! out binary-fetched-ch store txs (:id m)))
                       (<? (store-commits! store cvs)))))
        (>! in m)
        (recur (<? pub-ch))))))

(defn- fetched [store err-ch fetch-ch out]
  (go-loop-try> err-ch [{:keys [ids peer id] :as m} (<? fetch-ch)]
    (when m
      (info "fetch:" ids)
      (let [fetched (->> (go-for [id ids] [id (<? (-get-in store [id]))])
                         (async/into {})
                         <?)]
        (>! out {:type :fetch/edn-ack
                 :values fetched
                 :id id
                 :peer peer})
        (debug "sent fetched:" fetched)
        (recur (<? fetch-ch))))))

(defn- binary-fetched [store err-ch binary-fetch-ch out]
  (go-loop-try> err-ch [{:keys [id peer blob-id] :as m} (<? binary-fetch-ch)]
    (when m
      (info "binary-fetch:" id)
      (>! out {:type :fetch/binary-ack
               :value (<? (-bget store blob-id
                                 #?(:clj #(with-open [baos (ByteArrayOutputStream.)]
                                             (io/copy (:input-stream %) baos)
                                             (.toByteArray baos))
                                    :cljs identity)))
               :blob-id blob-id
               :id id
               :peer peer})
      (debug "sent blob " id ": " blob-id)
      (recur (<? binary-fetch-ch)))))


(defn- fetch-dispatch [{:keys [type] :as m}]
  (case type
    :pub/downstream :pub/downstream
    :fetch/edn :fetch/edn
    :fetch/edn-ack :fetch/edn-ack
    :fetch/binary :fetch/binary
    :fetch/binary-ack :fetch/binary-ack
    :unrelated))

(defn fetch [store err-ch [in out]]
  (let [new-in (chan)
        p (pub in fetch-dispatch)
        pub-ch (chan 100) ;; TODO disconnect on overflow?
        fetch-ch (chan)
        binary-fetch-ch (chan)]
    (sub p :pub/downstream pub-ch)
    (fetch-new-pub store err-ch p pub-ch [new-in out])

    (sub p :fetch/edn fetch-ch)
    (fetched store err-ch fetch-ch out)

    (sub p :fetch/binary binary-fetch-ch)
    (binary-fetched store err-ch binary-fetch-ch out)

    (sub p :unrelated new-in)
    [new-in out]))
