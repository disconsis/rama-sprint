(ns sprinter.sprint-module-test
  (:use [com.rpl.rama]
        [com.rpl.rama.path])
  (:require [com.rpl.rama.aggs :as aggs]
            [com.rpl.rama.ops :as ops]
            [com.rpl.rama.test :as rtest]
            [sprinter.sprint-module :as sut]
            [clojure.test :refer [deftest is run-tests]]))

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

      (is (sut/user-success?
           (foreign-append! *user-connects-depot
                            (sut/->UserConnect user-id-1 "ketan")))
          "connection try with an unused username should succeed")

      (is (= user-id-1 (foreign-select-one (keypath "ketan") $$username->id))
          "saved user id should be the one we registered with")

      (is (= "ketan" (foreign-select-one (keypath user-id-1 :user-name) $$users))
          "saved user id should map to the correct username")

      (is (sut/user-failure?
           (foreign-append! *user-connects-depot
                            (sut/->UserConnect user-id-2 "ketan")))
          "connection try with a taken username should fail")

      (is (= user-id-1 (foreign-select-one (keypath "ketan") $$username->id))
          "saved user id should be the one we registered with")

      (is (nil? (foreign-select-one (keypath user-id-2) $$users))
          "failed user id should not be saved")

      (is (sut/user-success?
           (foreign-append! *user-connects-depot
                            (sut/->UserConnect user-id-2 "foo"))))

      (is (sut/user-failure?
           (foreign-append! *user-connects-depot
                            (sut/->UserEdit user-id-2 :user-name "ketan")))
          "trying to change username to a taken one should fail")

      (is (= "foo" (foreign-select-one (keypath user-id-2 :user-name) $$users)))

      (is (sut/user-success?
           (foreign-append! *user-connects-depot
                            (sut/->UserEdit user-id-2 :user-name "bar")))
          "trying to change username to a an unused one should succeed")

      (is (= "bar" (foreign-select-one (keypath user-id-2 :user-name) $$users))
          "edit to username should be saved")

      (is (nil? (foreign-select-one (keypath "foo") $$username->id))
          "old name should be removed from the map")

      (is (some? (foreign-select-one (keypath "bar") $$username->id))
          "new name should be present in the map")
      )))

#_(run-tests)
