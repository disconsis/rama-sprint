(defproject disconsis/sprinter "1.0.0-SNAPSHOT"
  :dependencies [[com.rpl/rama-helpers "0.9.1"]]
  :repositories [["releases" {:id "maven-releases"
                              :url "https://nexus.redplanetlabs.com/repository/maven-public-releases"}]]

  :profiles {:dev {:resource-paths ["test/resources/"
                                    ;; this allows clj-kondo to pick up the latest rama config
                                    "rama-clj-kondo"]}
             :provided {:dependencies [[com.rpl/rama "0.11.4"]]}}
  )
