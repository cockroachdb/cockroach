// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { call } from "redux-saga/effects";
import { expectSaga, testSaga } from "redux-saga-test-plan";
import { throwError } from "redux-saga-test-plan/providers";

import { getAllMetricMetadata, MetricMetadataResponseMessage } from "src/util/api";

import {
  METRICS_METADATA_REQUEST,
  METRICS_METADATA_REQUEST_COMPLETE,
  METRICS_METADATA_REQUEST_ERROR,
} from "src/redux/metricsMetadata/actions";
import { requestMetricsMetadata } from "./saga";

describe("Metrics Metadata Saga", () => {

  const fakeMetricMetadataResponse: MetricMetadataResponseMessage = {
    toJSON() {
      return {};
    },
    metadata: {
      "metric1": {
        name: "metric1",
        help: "Help metric1",
        measurement: "measurement",
        unit: 1,
      },
      "metric2": {
        name: "metric2",
        help: "Help metric2",
        measurement: "measurement",
        unit: 1,
      },
    },
  };

  describe("Integration tests", () => {
    it("fetches metric metadata", () => {
      const requestError = new Error("Failed request");

      return expectSaga(requestMetricsMetadata, getAllMetricMetadata)
        .provide([
          [call(getAllMetricMetadata), throwError(requestError)],
        ])
        .put({
          type: METRICS_METADATA_REQUEST_ERROR,
          payload: requestError,
        })
        .dispatch({type: METRICS_METADATA_REQUEST})
        .run();
    });

    it("returns an error on failed request", () => {
      return expectSaga(requestMetricsMetadata, getAllMetricMetadata)
        .provide([
          [call(getAllMetricMetadata), fakeMetricMetadataResponse],
        ])
        .put({
          type: METRICS_METADATA_REQUEST_COMPLETE,
          payload: fakeMetricMetadataResponse.metadata,
        })
        .dispatch({type: METRICS_METADATA_REQUEST})
        .run();
    });
  });

  describe("Integration tests", () => {
    it("handles reducers actions", () => {
      return testSaga(requestMetricsMetadata)
        .next()
        .call(getAllMetricMetadata)
        .next(fakeMetricMetadataResponse)
        .put({
          type: METRICS_METADATA_REQUEST_COMPLETE,
          payload: fakeMetricMetadataResponse.metadata,
        });
    });
  });
});
