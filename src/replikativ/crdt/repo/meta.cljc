(ns replikativ.crdt.repo.meta
  "Operation on metadata and commit-graph (directed acyclic graph) of a repository.

   Metadata repository-format for automatic server-side
   synching (p2p-web). Have a look at the midje-doc documentation for
   more information."
  (:require [clojure.set :as set]))

(defn consistent-graph? [graph]
  (let [parents (->> graph vals (map set) (apply set/union))
        commits (->> graph keys set)]
    (set/superset? commits parents)))

(defn- track-returnpaths [returnpaths heads meta]
  (reduce (fn [returnpaths head]
            (reduce (fn [returnpaths parent]
                      (update-in returnpaths [parent] #(conj (or %1 #{}) %2) head))
                    returnpaths
                    (meta head)))
          returnpaths
          heads))


(defn- init-returnpath [heads]
  (reduce #(assoc %1 %2 #{}) {} heads))


(defn lowest-common-ancestors
  "Naive online BFS implementation. Assumes no cycles exist."
  ([meta-a heads-a meta-b heads-b]
     (let [heads-a (set heads-a)
           heads-b (set heads-b)
           returnpaths-a (init-returnpath heads-a)
           returnpaths-b (init-returnpath heads-b)
           cut (set/intersection heads-a heads-b)]
       (if-not (empty? cut) {:cut cut
                             :returnpaths-a returnpaths-a
                             :returnpaths-b returnpaths-b}
               (lowest-common-ancestors meta-a heads-a returnpaths-a
                                        meta-b heads-b returnpaths-b))))
  ([meta-a heads-a returnpaths-a
    meta-b heads-b returnpaths-b]
     (let [new-returnpaths-a (track-returnpaths returnpaths-a heads-a meta-a)
           new-returnpaths-b (track-returnpaths returnpaths-b heads-b meta-b)
           cut (set/intersection (set (keys new-returnpaths-a)) (set (keys new-returnpaths-b)))]
       (if (or (not (empty? cut))
               (and (empty? heads-a) (empty? heads-b)))
         {:cut cut :returnpaths-a new-returnpaths-a :returnpaths-b new-returnpaths-b}
         (let [new-heads-a (set (mapcat meta-a heads-a))
               new-heads-b (set (mapcat meta-b heads-b))]
           (recur meta-a new-heads-a new-returnpaths-a
                  meta-b new-heads-b new-returnpaths-b))))))


(defn- merge-parent [missing-returnpaths meta parent]
  (reduce (fn [meta child]
            (update-in meta [child] #(conj (or %1 []) %2) parent))
          meta
          (missing-returnpaths parent)))


(defn merge-ancestors
  "Use returnpaths and cut from lowest-common-ancestors to merge alien
   ancestor paths into meta data."
  ([meta cut missing-returnpaths]
     (let [new-meta (reduce (partial merge-parent missing-returnpaths) meta cut)
           new-cut (mapcat missing-returnpaths cut)]
       (if (empty? new-cut) new-meta
         (recur new-meta new-cut missing-returnpaths)))))


;; TODO refactor to isolate-tipps
(declare isolate-branch)
(defn isolate-branch                    ; -nomemo
  "Isolate a branch's metadata commit-graph."
  ([meta branch]
   (isolate-branch (:commit-graph meta) (-> meta :branches (get branch)) {}))
  ([commit-graph cut branch-meta]
   (if (empty? cut) branch-meta
       (recur commit-graph
              (set (mapcat commit-graph cut))
              (merge branch-meta (select-keys commit-graph cut))))))

(defn- old-heads [graph heads]
  (set (for [a heads b heads]
         (if (not= a b)                 ; => not a and b in cut
           (let [{:keys [returnpaths-a returnpaths-b]}
                 (lowest-common-ancestors graph #{a} graph #{b})
                 keys-a (set (keys returnpaths-a))
                 keys-b (set (keys returnpaths-b))]
             (cond (keys-b a) a
                   (keys-a b) b))))))


(defn remove-ancestors [graph heads-a heads-b]
  (if graph
    (let [to-remove (old-heads graph (set/union heads-a heads-b))]
      (set (filter #(not (to-remove %)) (set/union heads-a heads-b))))))

(defn downstream
  "Applies downstream updates from op to state. Idempotent and
  commutative."
  [{:keys [commit-graph branches] :as repo} op]
  (let [new-graph (merge (:commit-graph op) commit-graph)
        ;; TODO supply whole graph including history
        new-branches  (merge-with (partial remove-ancestors new-graph)
                                  branches (:branches op))]
    ;; TODO move check to entry point/middleware
    #_(when-not (consistent-graph? new-graph)
      (throw (ex-info "Remote meta does not have a consistent graph oder."
                      {:type :inconsistent-commit-graph
                       :op op
                       :graph new-graph})))
    (assoc repo
           :branches new-branches
           :commit-graph new-graph)))
