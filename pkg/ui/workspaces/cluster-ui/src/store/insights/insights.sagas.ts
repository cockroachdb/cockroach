// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { all, call, delay, put, takeLatest } from "redux-saga/effects";

import { actions } from "./insights.reducer";
import { getInsightEventState } from "src/api/insightsApi";
import { throttleWithReset } from "../utils";
import { rootActions } from "../reducers";

export function* refreshInsightsSaga() {
  yield put(actions.request());
}

export function* requestInsightsSaga(): any {
  try {
    const result = yield call(getInsightEventState);
    yield put(actions.received(result));
  } catch (e) {
    yield put(actions.failed(e));
  }
}

export function* receivedInsightsSaga(delayMs: number) {
  yield delay(delayMs);
  yield put(actions.invalidated());
}

export function* insightsSaga(cacheInvalidationPeriod: number = 10 * 1000) {
  yield all([
    throttleWithReset(
      cacheInvalidationPeriod,
      actions.refresh,
      [actions.invalidated, rootActions.resetState],
      refreshInsightsSaga,
    ),
    takeLatest(actions.request, requestInsightsSaga),
    takeLatest(actions.received, receivedInsightsSaga, cacheInvalidationPeriod),
  ]);
}
