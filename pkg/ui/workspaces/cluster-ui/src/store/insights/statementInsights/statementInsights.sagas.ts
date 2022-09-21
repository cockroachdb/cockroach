// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { all, call, delay, put, takeLatest } from "redux-saga/effects";

import { actions } from "./statementInsights.reducer";
import { getStatementInsightsApi } from "src/api/insightsApi";
import { throttleWithReset } from "../../utils";
import { rootActions } from "../../reducers";

export function* refreshStatementInsightsSaga() {
  yield put(actions.request());
}

export function* requestStatementInsightsSaga(): any {
  try {
    const result = yield call(getStatementInsightsApi);
    yield put(actions.received(result));
  } catch (e) {
    yield put(actions.failed(e));
  }
}

export function* receivedStatementInsightsSaga(delayMs: number) {
  yield delay(delayMs);
  yield put(actions.invalidated());
}

export function* statementInsightsSaga(
  cacheInvalidationPeriod: number = 10 * 1000,
) {
  yield all([
    throttleWithReset(
      cacheInvalidationPeriod,
      actions.refresh,
      [actions.invalidated, rootActions.resetState],
      refreshStatementInsightsSaga,
    ),
    takeLatest(actions.request, requestStatementInsightsSaga),
    takeLatest(
      actions.received,
      receivedStatementInsightsSaga,
      cacheInvalidationPeriod,
    ),
  ]);
}
