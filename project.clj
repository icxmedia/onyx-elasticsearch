(defproject com.icxmedia/onyx-elasticsearch "0.9.15.1-SNAPSHOT"
  :description "Onyx plugin for Elasticsearch"
  :url "https://github.com/onyx-platform/onyx-elasticsearch"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :plugins [[s3-wagon-private "1.2.0"]]
  :repositories {"snapshots" {:url "https://clojars.org/repo"
                              :username :env
                              :password :env
                              :sign-releases false}
                 "releases" {:url "https://clojars.org/repo"
                             :username :env
                             :password :env
                             :sign-releases false}
                 "icxmedia" {:url "s3p://maven.icxmedia.com/releases/"
                             :creds :gpg
                             :update :always}}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 ^{:voom {:repo "git@github.com:onyx-platform/onyx.git" :branch "master"}}
                 [org.onyxplatform/onyx "0.9.15"]
                 [clojurewerkz/elastisch "2.2.0"]]
  :profiles {:dev {:dependencies [[http-kit "2.1.19"]
                                  [org.clojure/data.json "0.2.6"]]
                   :plugins [[lein-set-version "0.4.1"]
                             [lein-update-dependency "0.1.2"]
                             [lein-pprint "1.1.1"]]}
             :circle-ci {:jvm-opts ["-Xmx4g"]}})
