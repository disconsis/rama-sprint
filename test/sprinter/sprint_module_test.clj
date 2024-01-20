(ns sprinter.sprint-module-test
  (:use [com.rpl.rama]
        [com.rpl.rama.path])
  (:require [com.rpl.rama.aggs :as aggs]
            [com.rpl.rama.ops :as ops]
            [com.rpl.rama.test :as rtest]
            [sprinter.sprint-module :as sut]
            [clojure.test :refer [deftest is]]))

(deftest users-test
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc sut/SprintModule {:tasks 4 :threads 2})
    (let [module-name (get-module-name sut/SprintModule)
          *user-connects-depot (foreign-depot ipc module-name "*user-connects-depot")
          *user-edits-depot (foreign-depot ipc module-name "*user-edits-depot")
          $$username->id (foreign-pstate ipc module-name "$$username->id")
          $$users (foreign-pstate ipc module-name "$$users")
          user-id-1 (random-uuid)
          user-id-2 (random-uuid)]

      (is (sut/success?
           (foreign-append! *user-connects-depot
                            (sut/->UserConnect user-id-1 "ketan")))
          "connection try with an unused username should succeed")

      (is (= user-id-1 (foreign-select-one (keypath "ketan") $$username->id))
          "saved user id should be the one we registered with")

      (is (= "ketan" (foreign-select-one (keypath user-id-1 :user-name) $$users))
          "saved user id should map to the correct username")

      (is (sut/failure?
           (foreign-append! *user-connects-depot
                            (sut/->UserConnect user-id-2 "ketan")))
          "connection try with a taken username should fail")

      (is (= user-id-1 (foreign-select-one (keypath "ketan") $$username->id))
          "saved user id should be the one we registered with")

      (is (nil? (foreign-select-one (keypath user-id-2) $$users))
          "failed user id should not be saved")

      (is (sut/success?
           (foreign-append! *user-connects-depot
                            (sut/->UserConnect user-id-2 "foo"))))

      (is (sut/failure?
           (foreign-append! *user-edits-depot
                            (sut/->UserEdit user-id-2 :user-name "ketan")))
          "trying to change username to a taken one should fail")

      (is (= "foo" (foreign-select-one (keypath user-id-2 :user-name) $$users)))

      (is (sut/success?
           (foreign-append! *user-edits-depot
                            (sut/->UserEdit user-id-2 :user-name "bar")))
          "trying to change username to a an unused one should succeed")

      (is (= "bar" (foreign-select-one (keypath user-id-2 :user-name) $$users))
          "edit to username should be saved")

      (is (nil? (foreign-select-one (keypath "foo") $$username->id))
          "old name should be removed from the map")

      (is (some? (foreign-select-one (keypath "bar") $$username->id))
          "new name should be present in the map")
      )))

(deftest user-expiration-test
  (with-redefs [sut/user-expiration-millis (* 5 1000)]
   (with-open [ipc (rtest/create-ipc)]
     (rtest/launch-module! ipc sut/SprintModule {:tasks 4 :threads 2})
     (let [module-name (get-module-name sut/SprintModule)
           *user-connects-depot (foreign-depot ipc module-name "*user-connects-depot")
           *project-creation-depot (foreign-depot ipc module-name "*project-creation-depot")
           $$users (foreign-pstate ipc module-name "$$users")
           $$username->id (foreign-pstate ipc module-name "$$username->id")
           $$projects (foreign-pstate ipc module-name "$$projects")
           user-id (random-uuid)
           user-id-2 (random-uuid)
           project-id (random-uuid)]

       (foreign-append! *user-connects-depot
                        (sut/->UserConnect user-id "ketan"))

       (foreign-append! *project-creation-depot
                        (sut/->ProjectCreate project-id "project" user-id))

       (Thread/sleep (* 3 1000))

       (is (some? (foreign-select-one (keypath user-id) $$users))
           "user should not be expired yet")

       (foreign-append! *user-connects-depot
                        (sut/->UserConnect user-id-2 "foo"))

       (Thread/sleep (* 5 1000))

       (is (some? (foreign-select-one (keypath user-id-2) $$users))
           "user-2 should not be expired yet")

       (is (nil? (foreign-select-one (keypath user-id) $$users))
           "user should be expired after some time")

       (is (nil? (foreign-select-one (keypath "ketan") $$username->id))
           "username should be removed after expiry")

       (is (nil? (foreign-select-one (keypath project-id) $$projects))
           "user's projects should be removed after expiry")))))

(deftest projects-test
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc sut/SprintModule {:tasks 4 :threads 2})
    (let [module-name (get-module-name sut/SprintModule)
          *user-connects-depot (foreign-depot ipc module-name "*user-connects-depot")
          user-id-1 (random-uuid)
          user-id-2 (random-uuid)
          *project-creation-depot (foreign-depot ipc module-name "*project-creation-depot")
          *project-edits-depot (foreign-depot ipc module-name "*project-edits-depot")
          project-id-1 (random-uuid)
          project-id-2 (random-uuid)]

      (foreign-append! *user-connects-depot
                       (sut/->UserConnect user-id-1 "user-1"))
      (foreign-append! *user-connects-depot
                       (sut/->UserConnect user-id-2 "user-2"))

      (is (sut/success?
           (foreign-append! *project-creation-depot
                            (sut/->ProjectCreate project-id-1 "proj-1" user-id-1)))
          "project creation succeeds")

      (is (sut/failure?
           (foreign-append! *project-creation-depot
                            (sut/->ProjectCreate project-id-2 "proj-1" user-id-1)))
          "one user can't create another project with the same name")

      (is (sut/success?
           (foreign-append! *project-creation-depot
                            (sut/->ProjectCreate project-id-2 "proj-1" user-id-2)))
          "other user can create another project with a used project name")

      (foreign-append! *project-creation-depot (sut/->ProjectCreate (random-uuid) "other-proj" user-id-1))

      (is (sut/failure?
           (foreign-append! *project-edits-depot
                            (sut/->ProjectEdit project-id-1 user-id-1 :project-name "other-proj")))
          "can't rename a project to something already used by user")

      (is (sut/success?
           (foreign-append! *project-edits-depot
                            (sut/->ProjectEdit project-id-2 user-id-2 :project-name "other-proj")))
          "user can rename project to the name of another user's project")

      (is (sut/success?
           (foreign-append! *project-edits-depot
                            (sut/->ProjectEdit project-id-1 user-id-1 :project-name "else-proj")))
          "can rename a project to an unused name"))))
