;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.
;;
;; Copyright (c) Andrey Antukh <niwi@niwi.nz>

(ns app.http.websocket
  "General purpose websockets interface."
  (:require
   [app.common.exceptions :as ex]
   [app.common.logging :as l]
   [app.common.transit :as t]
   [app.common.uuid :as uuid]
   [app.config :as cf]
   [app.util.async :as aa]
   [app.util.time :as dt]
   [integrant.core :as ig]
   [clojure.core.async :as a]
   [yetti.websocket :as yws])
  (:import
   java.nio.ByteBuffer))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; WEBSOCKET HANDLER
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; The websocket handler is defined by multimethod with implementation
;; for each posible event that websocket connection can receive.

(declare send-presence!)

(defmulti handle-message
  (fn [_ _ message] (:type message)))

(defmethod handle-message :connect
  [{:keys [msgbus file-id team-id session-id]} ws _]
  (let [sub-ch (a/chan (a/dropping-buffer 32))]
    (swap! ws assoc :sub-ch sub-ch)

    ;; Start a subscription forwarding goroutine
    (a/go-loop []
      (when-let [val (a/<! sub-ch)]
        (when-not (= (:session-id val) session-id)
          ;; If we receive a connect message of other user, we need
          ;; to send an update presence to all participants.
          (when (= :connect (:type val))
            (a/<! (send-presence cfg :presence)))

          ;; Then, just forward the message
          (a/>! out-ch val))
        (recur)))

    (a/go
      (a/<! (msgbus :sub {:topics [file-id team-id] :chan sub-ch}))
      (a/<! (send-presence! cfg :connect)))))

(defmethod handle-message :disconnect
  [cfg ws _]
  (a/close! (:sub-ch @ws))
  (send-presence! cfg :disconnect))

(defmethod handle-message :keepalive
  [_ _]
  (a/go :nothing))

(defmethod handle-message :pointer-update
  [{:keys [profile-id file-id session-id msgbus] :as cfg} message]
  (let [message (assoc message
                       :profile-id profile-id
                       :session-id session-id)]
    (msgbus :pub {:topic file-id :message message})))

(defmethod handle-message :default
  [_ws message]
  (a/go
    (l/log :level :warn
           :msg "received unexpected message"
           :message message)))

;; --- IMPL

(defn send-presence!
  ([cfg] (send-presence cfg :presence))
  ([{:keys [msgbus session-id profile-id file-id]} type]
   (msgbus :pub {:topic file-id
                 :message {:type type
                           :session-id session-id
                           :profile-id profile-id}}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; GENERIC PROTOCOL WRAPPER
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; This function implements a websocket handler wrapper that already
;; has the basic protocol and connection lifecycle handling. It emits
;; websocket events to the specified method with decoded message.

(defn- ws-send!
  "Fully asynchronous websocket send operation."
  [ws s]
  (let [ch (a/chan 1)]
    (yws/send! ws s (fn [e]
                      (when e (a/offer! ch e))
                      (a/close! ch)))
    ch))

(defn- ws-ping!
  ([ws] (ws-ping! ws (ByteBuffer/allocate 0)))
  ([ws s]
   (let [ch (a/chan 1)]
     (yws/ping! ws s (fn [e]
                       (when e (a/offer! ch e))
                       (a/close! ch)))
     ch)))

(defn- start-input-loop
  [ws handler]
  (let [{:keys [input-ch output-ch close-ch]} @ws]
    (a/go
      (a/<! (handler ws {:type :connect}))
      (a/<! (a/go-loop []
              (when-let [request (a/<! input-ch)]
                (let [[val port] (a/alts! [(handler ws request) close-ch])]
                  (when-not (= port close-ch)
                    (cond
                      (ex/ex-info? response)
                      (a/>! output-ch {:type :error :error (ex-data response)})

                      (ex/exception? response)
                      (a/>! output-ch {:type :error :error {:message (ex-message response)}})

                      (map? response)
                      (a/>! output-ch response))

                    (recur))))))
      (a/<! (handler ws {:type :disconnect})))))

(defn- start-output-loop
  [{:keys [conn output-ch input-ch]}]
  (a/go-loop []
    (when-let [val (a/<! output-ch)]
      (a/<! (ws-send! conn (t/encode-str val)))
      (recur))))

(defn- start-ping-pong-loop
  [{:keys [conn output-ch input-ch close-ch on-close pong]}]
  (let [beats (atom #{})

        encode-beat
        (fn [n]
          (doto (ByteBuffer/allocate 8)
            (.putLong i)))

        decode-beat
        (fn [^ByteBuffer buffer]
          (when (= 8 (.capacity buffer))
            (.rewind buffer)
            (.getLong buffer)))]

    (a/go-loop [i 0]
      (let [[val port] (a/alts! [close-ch (a/timeout 1000)])]
        (when (and (yws/connected? conn) (not= port close-ch))
          (a/<! (ws-ping! conn (encode-beat i)))
          (let [issued (swap! beats conj (long i))]
            (if (>= issued 8)
              (on-close conn -1 "pong-timeout")
              (recur (inc i)))))))

    (a/go-loop []
      (when-let [buffer (a/<! pong-ch)]
        (swap! beats disj (decode-beat buffer))))))

(defn wrap
  ([handler] (wrap handler {}))
  ([handler {:keys [input-buff-size output-buff-size]
             :or {input-buff-size 32
                  output-buff-size 32}}]
   (fn [request]
     (let [input-ch  (a/chan input-buff-size)
           output-ch (a/chan output-buff-size)
           pong-ch   (a/chan (a/sliding-buffer 6))
           close-ch  (a/chan)

           on-error
           (fn [conn err]
             (l/warn :hint "on-error" :err (str err))
             (a/close! close-ch)
             (a/close! pong-ch)
             (a/close! output-ch)
             (a/close! input-ch))

           on-close
           (fn [conn status reason]
             (l/info :hint "on-close" :status status :reason reason)
             (a/close! close-ch)
             (a/close! pong-ch)
             (a/close! output-ch)
             (a/close! input-ch))

           on-connect
           (fn [conn]
             (l/info :hint "on-connect" :client (yws/remote-addr conn))
             (let [ws (atom {:output-ch output-ch
                             :input-ch input-ch
                             :close-ch close-ch
                             :conn conn})]

               ;; Properly handle keepalive
               ;; (yws/idle-timeout! conn (dt/duration 10000))
               ;; (-> @ws
               ;;     (assoc :pong-ch pong-ch)
               ;;     (assoc :on-close on-close)
               ;;     (start-ping-pong-loop))

               ;; Forward all messages from output-ch to the websocket
               ;; connection
               (start-output-loop @ws)

               ;; React on messages received from the client
               (start-input-loop ws handler)))

           on-message
           (fn [conn message]
             (let [message (t/decode-str message)]
               (l/debug :hint "on-message" :message message)
               (when-not (a/offer! input-ch message)
                 (l/warn :hint "drop messages"))))

           on-ping
           (fn [conn buffer]
             ;; (l/debug :hint "on-ping" :buffer buffer)
             )

           on-pong
           (fn [conn buffer]
             ;; (l/debug :hint "on-pong" :buffer buffer)
             (a/offer! pong-ch buffer))
           ]
       {:on-connect on-connect
        :on-error on-error
        :on-close on-close
        :on-text on-message
        :on-ping on-ping
        :on-pong on-pong}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; HTTP HANDLER
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(declare retrieve-file)

(s/def ::file-id ::us/uuid)
(s/def ::session-id ::us/uuid)
(s/def ::handler-params
  (s/keys :req-un [::file-id ::session-id]))

(defn- handler
  [{:keys [pool] :as cfg} {:keys [profile-id params] :as req}]
  (let [params (us/conform ::handler-params params)
        file   (retrieve-file pool (:file-id params))
        cfg    (merge cfg params
                      {:profile-id profile-id
                       :team-id (:team-id file)})]

    (when-not profile-id
      (ex/raise :type :authentication
                :hint "Authentication required."))

    (when-not file
      (ex/raise :type :not-found
                :code :object-not-found))

    (when-not (yws/upgrade-request? req)
      (ex/raise :type :validation
                :code :websocket-request-expected
                :hint "this endpoint only accepts websocket connections"))

    (yws/upgrade req (wrap (partial handle-message cfg)))))

(def ^:private
  sql:retrieve-file
  "select f.id as id,
          p.team_id as team_id
     from file as f
     join project as p on (p.id = f.project_id)
    where f.id = ?")

(defn- retrieve-file
  [conn id]
  (db/exec-one! conn [sql:retrieve-file id]))


(s/def ::session map?)
(s/def ::msgbus fn?)

(defmethod ig/pre-init-spec ::handler [_]
  (s/keys :req-un [::msgbus ::db/pool ::session ::mtx/metrics ::wrk/executor]))

(defmethod ig/init-key ::handler
  [_ {:keys [session metrics] :as cfg}]
  (let [mtx-active-connections
        (mtx/create
         {:name "websocket_active_connections"
          :registry (:registry metrics)
          :type :gauge
          :help "Active websocket connections."})

        mtx-messages
        (mtx/create
         {:name "websocket_message_total"
          :registry (:registry metrics)
          :labels ["op"]
          :type :counter
          :help "Counter of processed messages."})

        mtx-sessions
        (mtx/create
         {:name "websocket_session_timing"
          :registry (:registry metrics)
          :quantiles []
          :help "Websocket session timing (seconds)."
          :type :summary})]

    (-> cfg
        (assoc :mtx-active-connections mtx-active-connections)
        (assoc :mtx-messages mtx-messages)
        (assoc :mtx-sessions mtx-sessions)
        (->> (partial handler)))))

