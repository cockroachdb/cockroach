// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { takeEvery } from "redux-saga";
import { call, put } from "redux-saga/effects";

import { getAllMetricMetadata } from "src/util/api";

import { METRICS_METADATA_REQUEST, metricsMetadataActions } from "./actions";

export function* requestMetricsMetadata() {
  try {
    const response = yield call(getAllMetricMetadata);
    yield put(metricsMetadataActions.requestCompleted(response.metadata));
  } catch (error) {
    yield put(metricsMetadataActions.requestError(error));
  }
}

export function* watchRequestMetricsMetadata() {
  yield takeEvery(METRICS_METADATA_REQUEST, requestMetricsMetadata);
}
