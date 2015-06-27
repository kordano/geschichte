(ns doc.intro
  (:require [midje.sweet :refer :all]
            [geschichte.environ :refer [*id-fn* *date-fn*]]
            [geschichte.crdt.repo.repo :as repo]
            [geschichte.crdt.repo.meta :as meta]
            [geschichte.crdt.repo.stage :as s]))

[[:chapter {:tag "motivation" :title "Motivation for geschichte"}]]

"The web is still a bag of data silos (*places* in Rich Hickey's terms). Despite existing cooperation on source code, data rarely is shared cooperatively, because it is accessed through a single (mostly proprietary) service, which also is fed with inputs to 'update' the data (read: it has an *API*). This creates a single point of perception to decide upon writes, which at the same time has to be economically viable and hence locks the data in.

While sophisticated new functional databases like [Datomic](http://www.datomic.com/) promise scalable relational programming and access to all data for the service provider, they still do not fit for distributed data. A single writer with a singular notion of time is still required. *geschichte* tries to apply some lessons learned from these efforts, building foremost on immutablity, but applies them to a different spot in the spectrum of storage management. The goal of geschichte is to build a distributed web and edit data collectively, while still allowing the right to fork and dissent for anybody. In general distributed 'serverless' applications should be possible. The tradeoff is application-specific/user-guided conflict resolution through three-way merges.

Experience with immutable bit-torrent sharing, distributed systems like git and mercurial, [Votorola](http://zelea.com/project/votorola/home.html) as well as [HistoDB](https://github.com/mirkokiefer/HistoDB) and its [description](https://github.com/mirkokiefer/syncing-thesis) have been inspirations. [CRDTs](http://hal.inria.fr/docs/00/55/55/88/PDF/techreport.pdf) have been introduced to allow carefree synching of metadata values without 'places'.

Github already resembles a community using tools to produce source code, but it is still a central site (service) and does not apply to the web itself. geschichte will use *p2p* web-technologies, like *websockets* and *webrtc*, to globally exchange values. It will also use `IndexedDB` in the browser. It is supposed to be *functionally pure* (besides synching io) and runs on `Clojure/ClojureScript`(/ClojureX?). Different io-techniques can be used to exchange *pub-sub* like update-notifications and allow immutable value fetches (*kv-store*). On the JVM it could hook into existing distributed systems beyond the web.

In the following we will explain how *geschichte* works by building a small repository containing tagged bookmarks as an example."

[[:chapter {:tag "usage" :title "Usage"}]]

[[:section {:title "Repository format"}]]

"Metadata (without id binding) looks like:"

{:causal-order
 {#uuid "214bd0cd-c737-4c7e-a0f5-778aca769cb7" []},
 :public false,
 :branches {"master" #{#uuid "214bd0cd-c737-4c7e-a0f5-778aca769cb7"}},
 :id #uuid "b1732275-a7e7-4401-9485-c7249e4a13e7",
 :description "Bookmark collection."},

"We need to rebind the *id-generating* function and *time-function* to have fixed values here for testing. Otherwise ids are cryptographic `UUID`s over their referenced values by default, so they cannot and may not conflict. UUID-5 is the only option if you want to conform to the standard, so **don't** rebind these functions. The UUIDs represent unchangable and global values. While time can be helpful to track in commits, it is not critical for synching metadata. We will zero it out for testing here. All function calls are in fact unit tests, you can run this documentation with *[midje-doc](http://docs.caudate.me/lein-midje-doc/)*."



(defn zero-date-fn [] (java.util.Date. 0))

(defn test-env [f]
  (binding [*id-fn* (let [counter (atom 0)]
                      (fn ([] (swap! counter inc))
                        ([val] (swap! counter inc))))
            *date-fn* zero-date-fn]
    (f)))

"First we need to create the repository. The new-repositroy function returns a map containing both the metadata and value of the new repository."



(fact
 (test-env
  #(repo/new-repository "author@mail.com"
                        "Bookmark collection."))
 =>
 {:state
  {:causal-order {1 []},
   :branches {"master" #{1}}},
  :transactions {"master" []},
  :downstream
  {:crdt :geschichte.repo
   :description "Bookmark collection.",
   :public false,
   :op {:method :new-state
        :causal-order {1 []},
        :branches {"master" #{1}}
        :version 1}},
  :new-values
  {"master"
   {1
    {:transactions [],
     :parents [],
     :branch "master"
     :ts #inst "1970-01-01T00:00:00.000-00:00",
     :author "author@mail.com"}}}})


   [[:subsection {:title "Metadata"}]]

   "First we have a look at the metadata structure: "

   {:causal-order {1 []},
    :public false,
    :branches {"master" #{1}},
    :id 2,
    :description "Bookmark collection."}

   "* `:causal-order` contains the whole dependency graph for revisions and is the core data we use to resolve conflicts. It points reverse from head to the root commit of the repository, which is the only commit with an empty parent vector.
   * `:branches` tracks all heads of branches in the causal order, while
   * `:id` (UUID) is generated on creation and is constant for all forks.
   * `:public` marks whether access is restricted to the user him/herself."

   [[:subsection {:title "Convergent Replicated Data Type (CRDT)"}]]

   "It is noteworthy that the metadata is a [CRDT](http://hal.inria.fr/docs/00/55/55/88/PDF/techreport.pdf). Since it needs to be synched globally (in a key value store), it needs to converge to be eventual consistent. When it is, synching new versions of metadata from remote sources can happen gradually and consistently converging to the global state and values of the repository. "

   "For each key the CRDT update function for value and new-value is described:"

   (fact
    (meta/update
     { ;; only new keys (commits) can be added => (merge new-value value)
      :causal-order {1 []    ;; keys: G-SET
                     2 [1]}, ;; parental values don't change

      ;; (or value new-value) might only turn true (monotone)
      :public false,

      ;; keys: G-SET
      ;; values: similar to OR-SET,
      ;; (heads) are merged with lca which is commutative and idempotent,
      ;; heads cannot become empty
      :branches {"master" #{2}},

      ;; might never change (global id), immutable
      :id 2,

      ;; set on initialisation, bound to id, immutable documentation
      :description "Bookmark collection."}

     ;; new metadata information:
     {:op {:causal-order {1 []
                          2 [1]
                          3 [2]
                          1000 [1]},
           :public true,
           :branches {"master" #{3},
                      "future" #{1000}},
           :id 2,
           :description "Bookmark collection."}})
    => {:op {:causal-order {1 []
                            2 [1]
                            3 [2]
                            1000 [1]},
             :public true,
             :branches {"master" #{3},
                        "future" #{1000}},
             :id 2,
             :description "Bookmark collection."}
        :state {:causal-order {2 [1], 1 [], 3 [2], 1000 [1]},
                :public true,
                :branches {"master" #{3},
                           "future" #{1000}},
                :id 2,
                :description "Bookmark collection."}})

   "The most sophisticated operation is merging branch heads through lca,
   which is necessary to resolve stale branch heads. This operation has
   currently square complexity on number of heads per branch. Having many
   branches is not a problem, having branches with many heads is."

   "The operation is commutative: "

   (fact
    (meta/update
     ;; new metadata information:
     {:causal-order {1 []
                     2 [1]
                     3 [2]
                     1000 [1]},
      :public true,
      :branches {"master" #{3}
                 "future" #{1000}},
      :id 2,
      :description "Bookmark collection."}
     {:op {:causal-order {1 []
                          2 [1]},
           :public false,
           :branches {"master" #{2}},
           :id 2,
           :description "Bookmark collection."}})
    => {:op {:causal-order {1 []
                            2 [1]},
             :public false,
             :branches {"master" #{2}},
             :id 2,
             :description "Bookmark collection."}
        :state {:causal-order {2 [1], 1 [], 3 [2], 1000 [1]},
                :public true,
                :branches {"master" #{3},
                           "future" #{1000}},
                :id 2,
                :description "Bookmark collection."}})

   "And idempotent: "

   (fact
    (meta/update
     {:causal-order {1 []
                     2 []},
      :public false,
      :branches {"master" #{2}},
      :id 2,
      :description "Bookmark collection."}
     {:op {:causal-order {1 []
                          2 []},
           :public false,
           :branches {"master" #{2}},
           :id 2,
           :description "Bookmark collection."}})
    => {:op {:causal-order {1 []
                            2 []},
             :public false,
             :branches {"master" #{2}},
             :id 2,
             :description "Bookmark collection."}
        :state {:causal-order {1 []
                               2 []},
                :public false,
                :branches {"master" #{2}},
                :id 2,
                :description "Bookmark collection."}})

   "Which we have (hopefully) shown for each field of the metadata map individually above."

   [[:subsection {:title "Value"}]]

   {#uuid "04eb5b1b-4d10-5036-b235-fa173253089a"
    {:transactions [['(fn add-links [old params] (merge-with set/union old params)) ;; actually uuids pointing to fn and params
                     {:economy #{"http://opensourceecology.org/"}}]],
     :ts #inst "1970-01-01T00:00:00.000-00:00",
     :author "author@mail.com",
     :parents [2 3], ;; normally singular, with merge sequence of parent commits applied in ascending order.
     }}

   "The value consists of one or more transactions, each a pair of a parameter map (data) and a freely chosen data (code) to describe the transaction. The code needn't be freely evaled, but can be mapped to a limit set of application specific operations. That way it can be safely resolved via a hardcoded hash-map and will still be invariant to version changes in code. Read: You should use a literal code description instead of symbols where possible, even if this induces a small overhead."

   [[:section {:title "Forking and Pulling"}]]

   [[:subsection {:title "Forking (Cloning)"}]]

   "You always fork from a desired branch. More branches can be pulled separately. Only common causal-order (branch ancestors) is pulled. This yields a copy (clone) of the remote branch."

   (fact
    (test-env
     #(repo/fork {:causal-order {1 []
                                 3 [1]},
                  :branches {"master" #{1}
                             "politics-coll" #{3}},}
                 "master"
                 true
                 "Bookmark collection."))
    =>
    {:state
     {:causal-order {1 []},
      :branches {"master" #{1}}},
     :transactions {"master" []},
     :downstream
     {:crdt :geschichte.repo
      :description "Bookmark collection.",
      :public true
      :op {:method :new-state
           :causal-order {1 []},
           :branches {"master" #{1}}
           :version 1}}})

[[:subsection {:title "Pull"}]]

"Pulling happens much the same."

(fact
 (test-env
  #(repo/pull {:state {:causal-order {1 []},
                       :public false,
                       :branches {"master" #{1}},
                       :id 2,
                       :description "Bookmark collection."}
               :transactions {"master" []}}
              "master"
              {:causal-order {1 []
                              3 [1]
                              4 [3]},
               :public false,
               :branches {"master" #{4}
                          "politics-coll" #{3}},
               :id 2,
               :description "Bookmark collection."}
              4))
 =>
 {:downstream
  {:crdt :geschichte.repo
   :op {:causal-order {4 [3], 3 [1], 1 []},
        :method :pull
        :branches {"master" #{4}}
        :version 1}},
  :state
  {:causal-order {1 [], 3 [1], 4 [3]},
   :public false,
   :branches {"master" #{4}},
   :id 2,
   :description "Bookmark collection."},
  :transactions {"master" []}})


[[:section {:title "Branching, Committing and Merging"}]]

[[:subsection {:title "Branch"}]]

"Branching is possible from any commit and does not create a commit:"

(fact
 (test-env
  #(repo/branch {:state {:causal-order {10 []
                                        30 [10]
                                        40 [30]},
                         :public false,
                         :branches {"master" #{40}
                                    "politics-coll" #{30}},
                         :id 2,
                         :description "Bookmark collection."}
                 :transactions {"master" []}}
                "environ-coll"
                30))
 =>
 {:downstream {:crdt :geschichte.repo
               :op {:branches {"environ-coll" #{30}}
                    :method :branch
                    :version 1}},
  :state
  {:causal-order {10 [], 30 [10], 40 [30]},
   :public false,
   :branches
   {"environ-coll" #{30}, "politics-coll" #{30}, "master" #{40}},
   :id 2,
   :description "Bookmark collection."},
  :transactions {"environ-coll" [], "master" []}})


[[:subsection {:title "Commit"}]]

"You can commit against any branch head."

(fact (test-env
       #(repo/commit {:state {:causal-order {10 []
                                             30 [10]
                                             40 [30]},
                              :public false,
                              :branches {"master" #{40}
                                         "politics-coll" #{30}},
                              :id 2,
                              :description "Bookmark collection."}
                      :transactions {"politics-coll" [[{:economy
                                                        #{"http://opensourceecology.org/"}
                                                        :politics #{"http://www.economist.com/"}}
                                                       '(fn merge [old params] (merge-with set/union old params))]]
                                     "master" []}}
                     "author@mail.com"
                     "politics-coll"))
      =>
      {:new-values
       {"politics-coll"
        {3
         {:transactions [[1 2]],
          :ts #inst "1970-01-01T00:00:00.000-00:00",
          :branch "politics-coll"
          :parents [30],
          :author "author@mail.com"},
         2 '(fn merge [old params] (merge-with set/union old params)),
         1
         {:politics #{"http://www.economist.com/"},
          :economy #{"http://opensourceecology.org/"}}}},
       :downstream
       {:crdt :geschichte.repo
        :op {:method :commit
             :causal-order {3 [30]},
             :branches {"politics-coll" #{3}}
             :version 1}},
       :state
       {:causal-order {3 [30], 10 [], 30 [10], 40 [30]},
        :public false,
        :branches {"politics-coll" #{3}, "master" #{40}},
        :id 2,
        :description "Bookmark collection."},
       :transactions {"politics-coll" [], "master" []}})



[[:subsection {:title "Merge"}]]

"You can check whether a merge is necessary (the head branch has multiple heads):"

(facts (test-env
        #(repo/multiple-branch-heads?  {:causal-order {10 []
                                                       30 [10]
                                                       40 [10]},
                                        :public false,
                                        :branches {"master" #{40 30}
                                                   "politics-coll" #{30}},
                                        :id 2,
                                        :description "Bookmark collection."}
                                       "master"))
       => true)

"Merging is like pulling but resolving the causal-order of the conflicting head commits with a new commit, which can apply further corrections atomically. You have to supply the remote-metadata and a vector of parents, which are applied to the repository value in order before the merge commit."

(fact (test-env
       #(repo/merge {:state {:causal-order {10 []
                                            30 [10]
                                            40 [10]},
                             :public false,
                             :branches {"master" #{40}
                                        "politics-coll" #{30}},
                             :id 2,
                             :description "Bookmark collection."}
                     :transactions {"master" []}}
                    "author@mail.com"
                    "master"
                    {:causal-order {10 []
                                    20 [10]},
                     :public false,
                     :branches {"master" #{20}},
                     :id 2,
                     :description "Bookmark collection."}
                    [40 20]
                    []))
      =>
      {:new-values
       {"master"
        {1
         {:transactions [],
          :ts #inst "1970-01-01T00:00:00.000-00:00",
          :branch "master"
          :parents [40 20],
          :author "author@mail.com"}}},
       :downstream {:crdt :geschichte.repo
                    :op {:method :merge
                         :causal-order {1 [40 20]},
                         :branches {"master" #{1}}
                         :version 1}},
       :state
       {:causal-order {1 [40 20], 20 [10], 10 [], 30 [10], 40 [10]},
        :public false,
        :branches {"politics-coll" #{30}, "master" #{1}},
        :id 2,
        :description "Bookmark collection."},
       :transactions {"master" []}})

"When there are pending commits, you need to resolve them first as well."
(fact (test-env
       #(try
          (repo/merge {:state {:causal-order {10 []
                                              30 [10]
                                              40 [10]},
                               :public false,
                               :branches {"master" #{40}
                                          "politics-coll" #{30}},
                               :id 2,
                               :description "Bookmark collection."}
                       :transactions {"master" [[{:economy #{"http://opensourceecology.org/"}
                                                  :politics #{"http://www.economist.com/"}}
                                                 '(fn merge-bookmarks [old params]
                                                    (merge-with set/union old params))]]}}
                      "author@mail.com"
                      "master"
                      {:causal-order {10 []
                                      20 [10]},
                       :public false,
                       :branches {"master" #{20}},
                       :id 2,
                       :description "Bookmark collection."}
                      [40 20]
                      [])
          (catch clojure.lang.ExceptionInfo e
            (= (-> e ex-data :type) :transactions-pending-might-conflict))))
      => true)


"Have a look at the [synching API](synching.html) as well. Further documentation will be added, have a look at the
[test/geschichte/core_test.clj](https://github.com/ghubber/geschichte/blob/master/test/geschichte/core_test.clj) tests or
the [API docs](doc/index.html) for implementation details."
