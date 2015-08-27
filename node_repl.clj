(require 'cljs.repl)
(require 'cljs.build.api)
(require 'cljs.repl.node)

(cljs.build.api/build ["src" "test/dev"]
  {:main 'dev.client
   :output-to "test/dev/out/main.js"
   :verbose true})

(cljs.repl/repl (cljs.repl.node/repl-env)
  :watch "test/dev"
  :output-dir "test/dev/out")
