// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { all, call, put, takeLatest } from "redux-saga/effects";

import { actions } from "./statementInsights.reducer";
import { StmtInsightsReq, getStmtInsightsApi } from "src/api/stmtInsightsApi";
import { PayloadAction } from "@reduxjs/toolkit";

export function* refreshStatementInsightsSaga(
  action?: PayloadAction<StmtInsightsReq>,
) {
  yield put(actions.request(action?.payload));
}

export function* requestStatementInsightsSaga(
  action?: PayloadAction<StmtInsightsReq>,
): any {
  try {
    const result = yield call(getStmtInsightsApi, action?.payload);
    yield put(actions.received(result));
  } catch (e) {
    yield put(actions.failed(e));
  }
}

export function* statementInsightsSaga() {
  yield all([
    takeLatest(actions.refresh, refreshStatementInsightsSaga),
    takeLatest(actions.request, requestStatementInsightsSaga),
  ]);
}
