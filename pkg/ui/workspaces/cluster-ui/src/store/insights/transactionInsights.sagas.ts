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

import { actions } from "./transactionInsights.reducer";
import { getTransactionInsightEventState } from "src/api/insightsApi";
import { throttleWithReset } from "../utils";
import { rootActions } from "../reducers";

export function* refreshTransactionInsightsSaga() {
  yield put(actions.request());
}

export function* requestTransactionInsightsSaga(): any {
  try {
    const result = yield call(getTransactionInsightEventState);
    yield put(actions.received(result));
  } catch (e) {
    yield put(actions.failed(e));
  }
}

export function* receivedTransactionInsightsSaga(delayMs: number) {
  yield delay(delayMs);
  yield put(actions.invalidated());
}

export function* transactionInsightsSaga(
  cacheInvalidationPeriod: number = 10 * 1000,
) {
  yield all([
    throttleWithReset(
      cacheInvalidationPeriod,
      actions.refresh,
      [actions.invalidated, rootActions.resetState],
      refreshTransactionInsightsSaga,
    ),
    takeLatest(actions.request, requestTransactionInsightsSaga),
    takeLatest(
      actions.received,
      receivedTransactionInsightsSaga,
      cacheInvalidationPeriod,
    ),
  ]);
}
