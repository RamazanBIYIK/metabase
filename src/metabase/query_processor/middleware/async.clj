(ns metabase.query-processor.middleware.async
  "Middleware for implementing async QP behavior."
  (:require [clojure.core.async :as a]
            [clojure.tools.logging :as log]
            [metabase.async.util :as async.u]
            [metabase.util.i18n :refer [trs]]))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                  async->sync                                                   |
;;; +----------------------------------------------------------------------------------------------------------------+

(defn async->sync
  "Async-style (4-arg) middleware that wraps the synchronous (1-arg) portion of the QP middleware."
  [qp]
  (fn [query respond raise canceled-chan]
    (a/go
      (when (a/<! canceled-chan)
        (println "[[canceled]]")        ; NOCOMMIT
        ))
    (if (a/poll! canceled-chan)
      (log/debug (trs "Request already canceled, will not run synchronous QP code."))
      (try
        (respond (qp query))
        (catch Throwable e
          (raise e))))))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                  async-setup                                                   |
;;; +----------------------------------------------------------------------------------------------------------------+

(defn- async-args []
  (let [out-chan      (a/promise-chan)
        canceled-chan (async.u/promise-canceled-chan out-chan)
        respond       (fn [result]
                        (println "(instance? InterruptedException result):" (instance? InterruptedException result)) ; NOCOMMIT
                        ;; result might be `nil` if query gets canceled
                        (if (some? result)
                          (a/>!! out-chan result))
                        (println "close!") ; NOCOMMIT
                        (a/close! out-chan))
        raise         (fn [e]
                        (println "(instance? InterruptedException e):" (instance? InterruptedException e)) ; NOCOMMIT
                        (log/warn e (trs "Unhandled exception, exepected `catch-exceptions` middleware to handle it."))
                        (respond e))]
    {:out-chan out-chan, :canceled-chan canceled-chan, :respond respond, :raise raise}))

(defn- wait-for-result [out-chan]
  ;; TODO - there should probably be some sort of max timeout here for out-chan. At least for test/dev purposes
  (let [result (a/<!! out-chan)]
    (if (instance? Throwable result)
      (throw result)
      result)))

(defn async-setup
  "Middleware that creates the output/canceled channels for the asynchronous (4-arg) QP middleware and runs it.

  Our 4-arg middleware follows the same pattern as async 3-arg Ring middleware, with the addition of fourth
  `canceled-chan` arg; this is a core.async channel that can be listened to to implement special query cancelation
  behavior, such as canceling JDBC queries. If the output channel is closed before the query completes (i.e., API
  request is canceled) this channel will receive a message; otherwise it will close whenever the output channel
  closes."
  [qp]
  (fn [{:keys [async?], :as query}]
    (let [{:keys [out-chan respond raise canceled-chan]} (async-args)]
      (try
        (qp query respond raise canceled-chan)
        ;; if query is `async?` return the output channel; otherwise block until output channel returns a result
        (if async?
          out-chan
          (wait-for-result out-chan))
        (catch Throwable e
          (raise e))))))
