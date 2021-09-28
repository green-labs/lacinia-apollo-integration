(ns greenlabs.apollo-helper
  (:require [camel-snake-kebab.core :as csk]
            [camel-snake-kebab.extras :as cske]
            [clj-http.client :as client]
            [clojure.data.json :as json]
            [clojure.walk :as walk]
            [overtone.at-at :as at-at]
            [protobuf.core :as protobuf])
  (:import
   (java.time ZonedDateTime ZoneId)
   (greenlabs.protobuf Report)))

(def ^:private apollo-studio-api-key (atom nil))
(def ^:private pool (atom nil))
(def ^:private tracings (atom []))

(defn- ->protobuf-bytes
  [message]
  (protobuf/->bytes (protobuf/create Report message)))

#_(defn- protobuf-bytes->
    [message]
    (protobuf/bytes-> (protobuf/create Report) message))

(defn- make-query
  [query options]
  (if-let [opration-name (:operation-name options)]
    (str "# " opration-name "\n" query)
    query))

(defn- transform-path
  [path]
  (walk/prewalk (fn [v]
                  (if (map? v)
                    (let [fields [:response-name :type :start-time :end-time :parent-type
                                  :index :child
                                  :error :message :location :line :column :json]
                          default (select-keys v fields)
                          others (apply dissoc v fields)
                          first-key (first (keys others))]
                      (cond
                        (keyword? first-key) (assoc default :child (vals others))
                        (int? first-key) (assoc default :child (map (fn [[k v]] {:index k :child (vals v)}) others))
                        :else v))
                    v))
                path))

(defn- resolvers->trace-root
  [resolvers]
  (->> resolvers
       (reduce (fn [m v] (assoc-in m (:path v) {:response-name (name (:field-name v))
                                                :type (:return-type v)
                                                :start-time (:start-offset v)
                                                :end-time (+ (:start-offset v) (:duration v))
                                                :parent-type (name (:parent-type v))}))
               {})
       transform-path))

(defn- tracing->trace
  [trace {:keys [tracing]}]
  (let [tracing (walk/postwalk #(cske/transform-keys csk/->kebab-case-keyword %) tracing)]
    #_(def *tracing tracing)
    (assoc trace :root (resolvers->trace-root (get-in tracing [:execution :resolvers]))
           :start-time (ZonedDateTime/parse (:start-time tracing))
           :end-time (ZonedDateTime/parse (:end-time tracing))
           :duration-ns (:duration tracing))))

(defn- ex->trace-root
  [ex]
  (let [info (ex-data ex)
        error {:message (.getMessage ex)
               :location (:location info)
               :json (json/write-str (or (:arguments info) ""))}]
    (->> (assoc-in {} (:path info) {:response-name (name (:field-name info))
                                    :error         [error]})
         transform-path)))

(defn- exception->trace
  [trace {:keys [ex start-time end-time]}]
  (assoc trace :root (ex->trace-root ex)
         :start-time (.withZoneSameInstant start-time (ZoneId/of "UTC"))
         :end-time (.withZoneSameInstant end-time (ZoneId/of "UTC"))))

(defn- ->time-map
  [t]
  {:seconds (.toEpochSecond t)
   :nanos (.getNano t)})

(defn- ->trace
  [m]
  (let [f (case (:type m)
            :tracing tracing->trace
            :error exception->trace)]
    (-> {:http {:method :post}}
        (f m)
        (update :start-time ->time-map)
        (update :end-time ->time-map))))

(defn- ->query-trace
  [{:keys [query options] :as m}]
  {:query (make-query query options)
   :trace (->trace m)})

(defn- query-traces->traces-per-query
  [query-traces]
  (->> query-traces
       (group-by :query)
       (map (fn [[query traces]]
              {:key query
               :value {:trace (map :trace traces)}}))))

(defn send-metric!
  [apollo-studio-api-key coll]
  (let [body (->> coll
                  (map ->query-trace)
                  query-traces->traces-per-query)]
    (client/post "https://usage-reporting.api.apollographql.com/api/ingress/traces"
                 {:headers          {:X-Api-Key apollo-studio-api-key}
                  :body             (->protobuf-bytes {:traces-per-query body})
                  :async?           false
                  :throw-exceptions false})))

(defn conj-tracing
  [query options tracing]
  (when @apollo-studio-api-key
    (swap! tracings conj {:type :tracing :query query :options options :tracing tracing})))

(defn conj-error
  [query options ex start-time]
  (when @apollo-studio-api-key
    (swap! tracings conj {:type :error :query query :options options :ex ex
                          :start-time start-time :end-time (ZonedDateTime/now)})))

(defn send-tracings!
  ([] (send-tracings! @apollo-studio-api-key))
  ([apollo-studio-api-key]
   (let [cnt (count @tracings)]
     (when (< 0 cnt)
       (println "Apollo Studio send metric:" cnt)))
   (doseq [tracings (partition-all 20 @tracings)]
     (send-metric! apollo-studio-api-key tracings))
   (reset! tracings [])))

(defn enable
  [api-key]
  (reset! apollo-studio-api-key api-key)
  (when-not @pool
    (reset! pool (at-at/mk-pool))))

(defn enabled?
  []
  (not (nil? @apollo-studio-api-key)))

(defn start
  []
  (when (enabled?)
    (at-at/every 20000 send-tracings! @pool :initial-delay 1000)))

(defn stop
  []
  (when @pool
    (at-at/stop-and-reset-pool! @pool :strategy :kill)
    ;; 남아있는 데이터가 있으면 마저 전송하도록 호출 함
    (send-tracings!)))

(comment
  (ZonedDateTime/parse "2021-09-24T05:17:33.861903Z")
  (assoc-in {} ["a" "b" "c"] 1)
  #_(resolvers->trace-root (get-in *tracing [:execution :resolvers]))
  #_(reduce (fn [m v] (assoc-in m (:path v) {:response-name (name (:field-name v))
                                             :type (:return-type v)
                                             :start-time (:start-offset v)
                                             :end-time (+ (:start-offset v) (:duration v))
                                             :parent-type (name (:parent-type v))}))
            {}
            (get-in *tracing [:execution :resolvers]))
  (ex->trace-root (ex-info (str "Exception in resolver for `Query/weatherGPSClj': test")
                           {:field-name :Query/weatherGPSClj
                            :arguments {:input {:latitude 35.1998507, :longitude 128.9179899}}
                            :location {:line 2, :column 3}
                            :path [:weatherGPSClj]})))
