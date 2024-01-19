(ns sprinter.sprint-module
  (:use [com.rpl.rama]
        [com.rpl.rama.path])
  (:require [com.rpl.rama.aggs :as aggs]
            [com.rpl.rama.ops :as ops]
            ;; stop clj-kondo complaining about undefined vars
            [com.rpl.rama :refer [ack-return>]])
  (:import (clojure.lang Keyword)
           (java.util UUID)))


;; Since stream topologies have atleast-once semantics, we need the user to
;; submit a user-id to handle the case where the same UserConnect event is
;; given to us multiple times. If both user-name and user-id are equal, it
;; means that the same user connect request is being retried, not that
;; another user is trying to connect with the same user name.
;; We could do this with a microbatch topology, but low latency is important
;; here.
(defrecord UserConnect [user-id user-name])
(defrecord UserEdit [user-id field value])

(defrecord ProjectCreate [project-name])
(defrecord ProjectEdit [project-id field value])

(defn current-time []
  (System/currentTimeMillis))

(defmodule SprintModule [setup topologies]
  ;; receives UserConnect events
  (declare-depot setup *user-connects-depot (hash-by :user-name))
  ;; receives UserEdit events
  (declare-depot setup *user-edits-depot (hash-by :user-id))

  ;; receives ProjectCreate events
  (declare-depot setup *project-creation-depot (hash-by :project-name))
  ;; receives ProjectEdit events
  (declare-depot setup *project-edits-depot (hash-by :project-id))

  (let [s (stream-topology topologies "users")]
    (declare-pstate s $$username->id {String UUID})
    (declare-pstate s $$users {UUID (fixed-keys-schema
                                     {:user-name String
                                      :connected-at Long})})

    (<<sources s
      (source> *user-connects-depot :> {:keys [*user-name *user-id] :as *user-connect})
      (local-select> (keypath *user-name) $$username->id :> *curr-user-id)
      (<<if (and> (some? *curr-user-id)
                  (not= *user-id *curr-user-id))
        (println "could not accept" *user-connect "since username is taken with id" *curr-user-id)
        (ack-return> {:success false
                      :reason "username already taken"})
        (else>)
        (local-transform> [(keypath *user-name) (termval *user-id)]
                          $$username->id)
        (|hash *user-id)
        (identity (current-time) :> *time)
        (local-transform>
         [(keypath *user-id)
          (multi-path [:user-name (termval *user-name)]
                      [:connected-at (termval *time)])]
         $$users)
        (println "accepted user" *user-id *user-name *time)
        (ack-return> {:success true
                      :user-id *user-id})))))


(defn user-success? [result]
  (get-in result ["users" :success]))

(def user-failure?
  (complement user-success?))

;; ------------
;;   Dev area
;; ------------

;; (?<-
;;  (println "foo"))

;; (?<-
;;  (filter> (nil? (local-select> (keypath "ketan") {"ketan" (random-uuid)})))
;;  (println "connected"))
