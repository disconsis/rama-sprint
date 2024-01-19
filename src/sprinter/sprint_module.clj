(ns sprinter.sprint-module
  (:use [com.rpl.rama]
        [com.rpl.rama.path])
  (:require [com.rpl.rama.aggs :as aggs :refer [+last]]
            [com.rpl.rama.ops :as ops])
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

(defrecord ProjectCreate [project-id project-name user-id])
(defrecord ProjectEdit [project-id field value])

(defn current-time []
  (System/currentTimeMillis))

(defn nil-or-equal [value]
  (fn [other]
    (or (nil? other) (= value other))))

;; TODO expire user sessions and delete their projects

(defmodule SprintModule [setup topologies]
  ;; receives UserConnect events
  (declare-depot setup *user-connects-depot (hash-by :user-name))
  ;; receives UserEdit events
  (declare-depot setup *user-edits-depot (hash-by :user-id))

  ;; receives ProjectCreate events
  (declare-depot setup *project-creation-depot (hash-by :project-name))
  ;; receives ProjectEdit events
  (declare-depot setup *project-edits-depot (hash-by :project-id))

  (let [us (stream-topology topologies "users")
        ps (stream-topology topologies "projects")]
    (declare-pstate us $$username->id {String UUID})
    (declare-pstate us $$users {UUID (fixed-keys-schema
                                       {:user-name String
                                        :connected-at Long})})

    (<<sources us
      ;; user connection flow
      (source> *user-connects-depot :> {:keys [*user-name *user-id] :as *user-connect})
      (local-select> (keypath *user-name) $$username->id :> *curr-user-id)
      (<<if (and> (some? *curr-user-id) (not= *user-id *curr-user-id))
        (<<do
         (println "could not accept" *user-connect "since username is taken with id" *curr-user-id)
         (ack-return> {:success false
                       :reason "username already taken"}))
        (else>)
        (<<do
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
                       :user-id *user-id})))

      ;; user edit flow
      (source> *user-edits-depot :> {:keys [*user-id *field *value] :as *edit})
      (local-select> (keypath *user-id) $$users :> {*curr-user-name :user-name :as *curr-info})
      (<<cond
       (case> (nil? *curr-info))
       (ack-return> {:success false
                     :reason "user-id not found"})

       (case> (= (get *field *curr-info) *value))
       ;; no change required
       (ack-return> {:success true})

       (case> (= :user-name *field))
       (|hash *value)
       (<<if (some? (local-select> (keypath *value) $$username->id))
         (ack-return> {:success false
                       :reason "username already taken"})
         (else>)
         (<<do
          (local-transform> [(keypath *value) (termval *user-id)]
                            $$username->id)
          (|hash *curr-user-name)
          (local-transform> [(keypath *curr-user-name) NONE>]
                            $$username->id)
          (|hash *user-id)
          (local-transform> [(keypath *user-id *field) (termval *value)]
                            $$users)
          (ack-return> {:success true})))

       (default>)
       (local-transform> [(keypath *user-id *field) (termval *value)]
                         $$users)
       (ack-return> {:success true})))

    (declare-pstate ps $$user->projects {UUID (map-schema
                                               String ;; project-name
                                               UUID   ;; project-id
                                               {:subindex? true})})
    (declare-pstate ps $$projects {UUID (fixed-keys-schema
                                          {:project-name String
                                           :created-by UUID
                                           :created-at Long})})

    (<<sources ps
      (source> *project-creation-depot :> {:keys [*project-id *project-name *user-id] :as *project-create})
      (<<cond
       (case> (nil? (select> (keypath *user-id) $$users)))
       (ack-return> {:success false
                     :reason "unknown user-id"})

       (case> (select> [(keypath *user-id *project-name)
                        ;; we need to check if the old project id is equal to this one
                        ;; since that indicates that this is a retried request
                        (view (complement (nil-or-equal *project-id)))]
                       $$user->projects))
       (ack-return> {:success false
                     :reason "project name already used by user"})

       (default>)
       (|hash *user-id)
       (+compound $$user->projects {*user-id {*project-name (+last *project-id)}})
       (|hash *project-id)
       (local-transform> [(keypath *project-id)
                          (multi-path [:project-name (termval *project-name)]
                                      [:created-by (termval *user-id)]
                                      [:created-at (termval (current-time))])]
                         $$projects)
       (ack-return> {:success true
                     :project-id *project-id})))))


(defn user-success? [result]
  (get-in result ["users" :success]))

(def user-failure?
  (complement user-success?))

(defn project-success? [result]
  (get-in result ["projects" :success]))

(def project-failure?
  (complement project-success?))

;; ------------
;;   Dev area
;; ------------

;; (?<-
;;  (println "foo"))

;; (?<-
;;  (filter> (nil? (local-select> (keypath "ketan") {"ketan" (random-uuid)})))
;;  (println "connected"))
