(defproject io.replikativ/replikativ "0.1.0-SNAPSHOT"
  :description "A scalable distributive p2p system for convergent replicated data types."

  :url "http://github.com/replikativ/replikativ"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  
  :source-paths ["src"]

  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/clojurescript "1.7.107"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [com.cognitect/transit-clj "0.8.275"]
                 [com.cognitect/transit-cljs "0.8.225"]
                 [http-kit "2.1.19"]
                 [http.async.client "0.6.0"]
                 [es.topiq/full.async "0.2.8-beta1"]
                 [kordano/full.cljs.async "0.1.3-alpha"]
                 [es.topiq/hasch "0.3.0-beta3"]
                 [net.polyc0l0r/konserve "0.2.4-SNAPSHOT"]
                 [com.cemerick/piggieback "0.2.1"]
                 [weasel "0.7.0" :exclusions [org.clojure/clojurescript]]
                 [com.cemerick/piggieback "0.2.1"]
                 [com.taoensso/timbre "4.0.2"]]

  :profiles {:dev {:dependencies [[midje "1.6.3"]
                                  [com.cemerick/piggieback "0.2.1"]
                                  [org.clojure/tools.nrepl "0.2.10"]]}}
  
  :plugins [[lein-cljsbuild "1.1.0"]
           [lein-figwheel "0.3.7"]
            [codox "0.8.13"]]

  :codox {:sources ["src"]}

  :clean-targets ^{:protect false}["target" "out" "test/dev/client/out" "resources/public/js"]
  
  :cljsbuild
  {:builds
   [
    {:id "bdev"
     :source-paths ["test/dev/client"]
     :figwheel true
     :compiler
     {:main dev.client.core
      :asset-path "js/out"
      :output-to "resources/public/js/client.js"
      :output-dir "resources/public/js/out"
      :optimizations :none
      :pretty-print true}}
    {:id "dev"
     :source-paths ["src"]
     :compiler
     {:output-to "resources/public/js/main.js"
      :optimizations :simple
      :pretty-print true}}
    {:id "min"
     :source-paths ["src"]
     :compiler
     {:output-to "resources/public/js/replikativ_min.js"
      :optimizations :advanced
      :pretty-print false}}]}

  :documentation
  {:files {"index"
           {:input "test/doc/intro.clj"
            :title "repository API"
            :sub-title "An introduction to the Repository functionality."
            :author "christian weilbach"
            :email  "ch_weil topiq es"}
           "replication"
           {:input "test/doc/replicate.clj"
            :title "synching API"
            :sub-title "An introduction to the replication protocol."
            :author "christian weilbach"
            :email  "ch_weil topiq es"}
           "stage"
           {:input "test/doc/replicate.clj"
            :title "stage API"
            :sub-title "An introduction to the state API."
            :author "christian weilbach"
            :email  "ch_weil topiq es"}
           "hooks"
           {:input "test/doc/hooks.clj"
            :title "hooks API"
            :sub-title "An introduction to pull hooks."
            :author "christian weilbach"
            :email  "ch_weil topiq es"}}})
