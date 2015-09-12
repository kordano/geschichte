(ns replikativ.platform
  (:require [replikativ.platform-log :refer [debug info warn error]]
            [konserve.platform :refer [read-string-safe]]
            [cognitect.transit :as transit]
            [goog.net.WebSocket]
            [goog.events :as events]
            [cljs.core.async :as async :refer (take! put! close! chan)])
  (:require-macros [cljs.core.async.macros :refer [go go-loop]]))


(defn now []
  (js/Date.))


(defn client-connect!
  "Connects to url. Puts [in out] channels on return channel when ready.
Only supports websocket at the moment, but is supposed to dispatch on
protocol of url. read-opts is ignored on cljs for now, use the
platform-wide reader setup."
  [url err-ch tag-table]
  (let [host (.getDomain (goog.Uri. url))
        channel (goog.net.WebSocket. false)
        in (chan)
        out (chan)
        opener (chan)]
    (info "CLIENT-CONNECT" url)
    (doto channel
      (events/listen goog.net.WebSocket.EventType.MESSAGE
                     (fn [evt]
                       (let [reader (transit/reader :json)
                             fr (js/FileReader.)]
                         (set! (.-onload fr) #(put! in
                                                    (with-meta
                                                      (transit/read
                                                       reader
                                                       (js/String. (.. % -target -result)))
                                                      {:host host})))
                         (.readAsText fr (.-message evt)))))
      (events/listen goog.net.WebSocket.EventType.CLOSED
                     (fn [evt] (close! in) (.close channel) (close! opener)))
      (events/listen goog.net.WebSocket.EventType.OPENED
                     (fn [evt] (put! opener [in out]) (close! opener)))
      (events/listen goog.net.WebSocket.EventType.ERROR
                     (fn [evt] (error "ERROR:" evt) (close! opener)))
      (.open url))
    ((fn sender []
       (take! out
              (fn [m]
                (when m
                  (let [writer (transit/writer :json)]
                    (.send channel (js/Blob. #js [(transit/write writer m)])))
                  (sender))))))
    opener))
