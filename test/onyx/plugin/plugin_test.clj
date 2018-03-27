(ns onyx.plugin.plugin-test
  (:require [clojure.core.async :refer [>!! chan]]
            [clojure.test :refer [deftest is testing]]
            [clojurewerkz.elastisch.query :as q]
            [clojurewerkz.elastisch.rest
             [document :as esrd]
             [response :as esrsp]]
            onyx.api
            onyx.plugin.elasticsearch
            [onyx.util.helper :as u]
            [taoensso.timbre :refer [info]]))

;; ElasticSearch should be running locally on standard ports
;; (http: 9200, native: 9300) prior to running these tests

(def id (str (java.util.UUID/randomUUID)))

(use-fixtures
  :each (fn [f]
          (f)
          (u/delete-indexes (.toString id))))

(def zk-addr "127.0.0.1:2189")

(def es-host "127.0.0.1")

(def es-rest-port 9200)

(def es-native-port 9300)

(def env-config 
  {:onyx/tenancy-id id
   :zookeeper/address zk-addr
   :zookeeper/server? true
   :zookeeper.server/port 2189})

(def peer-config 
  {:onyx/tenancy-id id
   :zookeeper/address zk-addr
   :onyx.peer/job-scheduler :onyx.job-scheduler/greedy
   :onyx.messaging.aeron/embedded-driver? true
   :onyx.messaging/allow-short-circuit? false
   :onyx.messaging/impl :aeron
   :onyx.messaging/peer-port 40200
   :onyx.messaging/bind-addr "localhost"})

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def n-messages 7)

(def batch-size 20)

(def workflow [[:in :write-messages]])

(def catalog-base
  [{:onyx/name :in
    :onyx/plugin :onyx.plugin.core-async/input
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :write-messages
    :onyx/plugin :onyx.plugin.elasticsearch/write-messages
    :onyx/type :output
    :onyx/medium :elasticsearch
    :elasticsearch/host es-host
    :elasticsearch/cluster-name (u/es-cluster-name es-host es-rest-port)
    :elasticsearch/http-ops {}
    :elasticsearch/index id
    :elasticsearch/mapping "_default_"
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Writes documents to elasticsearch"}])

(def catalog-http&write
  [(first catalog-base)
   (merge
    (second catalog-base)
    {:elasticsearch/port es-rest-port
     :elasticsearch/client-type :http
     :elasticsearch/write-type :insert})])

(def in-chan-http (chan (inc n-messages)))

(defn inject-in-ch-http [_ _]
  {:core.async/chan in-chan-http})

(def in-calls-http
  {:lifecycle/before-task-start inject-in-ch-http})

(def lifecycles-http
  [{:lifecycle/task :in
    :lifecycle/calls ::in-calls-http}
   {:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :write-messages
    :lifecycle/calls :onyx.plugin.elasticsearch/write-messages-calls}])

(def v-peers (onyx.api/start-peers 2 peer-group))

(defn run-job
  [name ch lc catalog & segments]
  (println "Running job")
  (doseq [seg segments] (>!! ch seg))
  (>!! ch :done)
  (let [job-info (onyx.api/submit-job
                  peer-config
                  {:catalog catalog
                   :workflow workflow
                   :lifecycles lc
                   :task-scheduler :onyx.task-scheduler/balanced})]
    (info (str "Awaiting job completion for " name))
    (println "Awaiting job completion")
    (onyx.api/await-job-completion peer-config (:job-id job-info))))

(deftest write-test
  (run-job
   "HTTP Client Job with Explicit Write Type"
   in-chan-http
   lifecycles-http
   catalog-http&write
   {:name "http:insert_plain-msg_noid" :index "one"}
   {:elasticsearch/message {:name "http:insert_detail-msg_id"} :elasticsearch/doc-id "1"}
   {:elasticsearch/message {:name "http:insert_detail-msg_id" :new "new"} :elasticsearch/doc-id "1" :elasticsearch/write-type :upsert}
   {:elasticsearch/message {:name "http:upsert_detail-msg_id"} :elasticsearch/doc-id "2" :elasticsearch/write-type :upsert}
   {:elasticsearch/message {:name "http:upsert_detail-msg_noid" :index "two"} :elasticsearch/write-type :upsert}
   {:elasticsearch/message {:name "http:insert-to-be-deleted"} :elasticsearch/doc-id "3"}
   {:elasticsearch/doc-id "3" :elasticsearch/write-type :delete})

  (Thread/sleep 7000)

  (doseq [v-peer v-peers]
    (onyx.api/shutdown-peer v-peer))

  (onyx.api/shutdown-peer-group peer-group)

  (onyx.api/shutdown-env env)

  (let [conn (u/connect-rest-client)]
    (testing "Insert: plain message with no id defined"
      (let [res (esrd/search conn id "_default_" :query (q/match :index "one"))]
        (is (= 1 (esrsp/total-hits res)))
        (is (not-empty (first (esrsp/ids-from res))))))
    (let [res (esrd/search conn id "_default_" :query (q/term :_id "1"))]
      (testing "Insert: detail message with id defined"
        (is (= 1 (esrsp/total-hits res))))
      (testing "Update: detail message with id defined"
        (is (= "new" (-> (esrsp/hits-from res) first :_source :new)))))
    (testing "Upsert: detail message with id defined"
      (let [res (esrd/search conn id "_default_" :query (q/term :_id "2"))]
        (is (= 1 (esrsp/total-hits res)))))
    (testing "Upsert: detail message with no id defined"
      (let [res (esrd/search conn id "_default_" :query (q/match :index "two"))]
        (is (= 1 (esrsp/total-hits res)))
        (is (not-empty (first (esrsp/ids-from res))))))
    (testing "Delete: detail defined"
      (let [res (esrd/search conn id "_default_" :query (q/term :_id "3"))]
        (is (= 0 (esrsp/total-hits res)))))))
