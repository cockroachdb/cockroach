// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { all, call, put, takeLatest } from "redux-saga/effects";

import { actions } from "./schemaInsights.reducer";
import { getSchemaInsights } from "../../api";

export function* refreshSchemaInsightsSaga() {
  yield put(actions.request());
}

export function* requestSchemaInsightsSaga(): any {
  try {
    const result = yield call(getSchemaInsights);
    yield put(actions.received(result));
  } catch (e) {
    yield put(actions.failed(e));
  }
}

export function* schemaInsightsSaga() {
  yield all([
    takeLatest(actions.refresh, refreshSchemaInsightsSaga),
    takeLatest(actions.request, requestSchemaInsightsSaga),
  ]);
}
