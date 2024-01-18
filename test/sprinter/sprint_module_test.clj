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
          user-connects-depot (foreign-depot ipc module-name "*user-connects-depot")
          username->id (foreign-pstate ipc module-name "$$username->id")
          users (foreign-pstate ipc module-name "$$users")
          user-id-1 (random-uuid)
          user-id-2 (random-uuid)]

      (is (sut/user-connect-success?
           (foreign-append! user-connects-depot
                            {:user-id user-id-1 :user-name "ketan"}))
          "connection try with an unused username should succeed")

      (is (= user-id-1 (foreign-select-one (keypath "ketan") username->id))
          "saved user id should be the one we registered with")

      (is (= "ketan" (foreign-select-one (keypath user-id-1 :user-name) users))
          "saved user id should map to the correct username")

      (is (sut/user-connect-failure?
           (foreign-append! user-connects-depot
                            {:user-id user-id-2 :user-name "ketan"}))
          "connection try with a taken username should fail")

      (is (= user-id-1 (foreign-select-one (keypath "ketan") username->id))
          "saved user id should be the one we registered with")

      (is (nil? (foreign-select-one (keypath user-id-2) users))
          "failed user id should not be saved"))))

#_(run-tests)
