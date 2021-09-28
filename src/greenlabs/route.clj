(ns greenlabs.route
  (:require [com.walmartlabs.lacinia :as lacinia]
            [com.walmartlabs.lacinia.tracing :as tracing]
            [greenlabs.apollo-helper :as apollo-helper]
            [medley.core :refer [dissoc-in]])
  (:import (java.time ZonedDateTime)))

(defn execute
  [schema query variables context options]
  (let [start-time (ZonedDateTime/now)] ;; 오류 발생시 시작 시간 정보가 없어서 사용하기 위함
    (try
      (let [context (if (apollo-helper/enabled?)
                  (tracing/enable-tracing context)
                  context)
            response (lacinia/execute schema query variables context options)]
        (when-let [tracing (get-in response [:extensions :tracing])]
          (apollo-helper/conj-tracing query options tracing))
        (dissoc-in response [:extensions :tracing]))
      (catch Exception e
        (apollo-helper/conj-error query options e start-time)
        (throw e)))))
