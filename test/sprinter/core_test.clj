(ns sprinter.core-test
  (:use [clojure.test]
        [com.rpl.rama]
        [com.rpl.rama.path])
  (:require [com.rpl.rama.aggs :as aggs]
            [com.rpl.rama.ops :as ops]
            [com.rpl.rama.test :as rtest]
            [sprinter.sprint-module :as mm]))

(deftest module-test
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc mm/SprintModule {:tasks 4 :threads 2})
    (let [module-name (get-module-name mm/SprintModule)
          user-connects-depot (foreign-depot ipc module-name "*user-connects-depot")
          user-name->id (foreign-pstate ipc module-name "$$user-name->id")
          users (foreign-pstate ipc module-name "$$users")]
      (foreign-append! user-connects-depot {:user-name "ketan"})
      (foreign-append! user-connects-depot {:user-name "ketan"}))))
