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
(defrecord ProjectEdit [project-id user-id field value])

(defn current-time []
  (System/currentTimeMillis))

(defn nil-or-equal-to? [expected actual]
  (or (nil? actual) (= expected actual)))

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
      (source> *user-connects-depot :> {:keys [*user-name *user-id]})
      (local-select> (keypath *user-name) $$username->id :> *curr-user-id)
      (<<if (not (nil-or-equal-to? *user-id *curr-user-id))
        (ack-return> {:success false
                      :reason "username already taken"})
        (else>)
        (<<do
         (|hash *user-id)
         (local-transform> [(keypath *user-id)
                            (multi-path [:user-name (termval *user-name)]
                                        [:connected-at (termval (current-time))])]
                           $$users)

         ;; need to do this last since this is the first thing we check in case of failures
         (|hash *user-name)
         (local-transform> [(keypath *user-name) (termval *user-id)]
                           $$username->id)
         (ack-return> {:success true
                       :user-id *user-id})))

      ;; user edit flow
      (source> *user-edits-depot :> {:keys [*user-id *field *value]})
      (local-select> (keypath *user-id) $$users :> {*curr-user-name :user-name :as *curr-info})
      (<<cond
       (case> (nil? *curr-info))
       (ack-return> {:success false
                     :reason "user-id not found"})

       (case> (= (get *field *curr-info) *value))
       ;; no change required
       (ack-return> {:success true})

       (case> (= :user-name *field))
       (<<if (some? (select> (keypath *value) $$username->id))
         (ack-return> {:success false
                       :reason "username already taken"})
         (else>)
         (<<do
          (|hash *curr-user-name)
          (local-transform> [(keypath *curr-user-name) NONE>]
                            $$username->id)
          (|hash *user-id)
          (local-transform> [(keypath *user-id *field) (termval *value)]
                            $$users)
          ;; need to do this last since this is the first thing we check in case of failures
          (|hash *value)
          (local-transform> [(keypath *value) (termval *user-id)]
                            $$username->id)
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
      ;; project creation flow
      (source> *project-creation-depot :> {:keys [*project-id *project-name *user-id]})
      (<<cond
       (case> (nil? (select> (keypath *user-id) $$users)))
       (ack-return> {:success false
                     :reason "unknown user-id"})

       ;; we need to check if the old project id is equal to this one
       ;; since that indicates that this is a retried request
       (case> (not (nil-or-equal-to?
                    *project-id
                    (select> (keypath *user-id *project-name) $$user->projects))))
       (ack-return> {:success false
                     :reason "project name already used by user"})

       (default>)
       (|hash *project-id)
       (local-transform> [(keypath *project-id)
                          (multi-path [:project-name (termval *project-name)]
                                      [:created-by (termval *user-id)]
                                      [:created-at (termval (current-time))])]
                         $$projects)

       ;; need to do this last since this is the first thing we check in case of failures
       (|hash *user-id)
       (+compound $$user->projects {*user-id {*project-name (+last *project-id)}})
       (ack-return> {:success true
                     :project-id *project-id}))

      ;; project edit flow
      (source> *project-edits-depot :> {:keys [*project-id *user-id *field *value]})
      (select> (keypath *project-id) $$projects :> {:keys [*created-by] :as *curr-info})
      (<<cond
       (case> (nil? *curr-info))
       (ack-return> {:success false
                     :reason "project-id not found"})

       (case> (= :created-by *field))
       (ack-return> {:success false
                     :reason "can't change creator of project"})

       (case> (not= *created-by *user-id))
       (ack-return> {:success false
                     :reason "project can only be edited by creator"})

       (case> (= (get *field *curr-info) *value))
       ;; no change required
       (ack-return> {:success true})

       (case> (= :project-name *field))
       (<<if (some? (select> (keypath *user-id *value) $$user->projects))
         (ack-return> {:success false
                       :reason "another project by the new name already exists"})
         (else>)
         (<<do
          (|hash *project-id)
          (local-transform> [(keypath *project-id *field) (termval *value)] $$projects)
          ;; need to do this last since this is the first thing we check in case of failures
          (|hash *user-id)
          (local-transform> [(keypath *user-id)
                             (multi-path [(keypath (get :project-name *curr-info)) NONE>]
                                         [(keypath *value) (termval *project-id)])]
                            $$user->projects)
          (ack-return> {:success true})))

       (default>)
       (|hash *user-id)
       (local-transform> [(keypath *user-id *field) (termval *value)]
                         $$projects)
       (ack-return> {:success true})))))


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
