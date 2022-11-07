// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { all, call, put, takeLatest } from "redux-saga/effects";

import { actions } from "./statementInsightCounts.reducer";
import { getStatementInsightCountApi } from "src/api/insightsApi";

export function* refreshStatementInsightCountsSaga() {
  yield put(actions.request());
}

export function* requestStatementInsightCountsSaga(): any {
  try {
    const result = yield call(getStatementInsightCountApi);
    yield put(actions.received(result));
  } catch (e) {
    yield put(actions.failed(e));
  }
}

export function* statementInsightCountsSaga() {
  yield all([
    takeLatest(actions.refresh, refreshStatementInsightCountsSaga),
    takeLatest(actions.request, requestStatementInsightCountsSaga),
  ]);
}
