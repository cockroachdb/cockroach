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
