// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { all, call, put, takeEvery } from "redux-saga/effects";

import { actions } from "./transactionInsights.reducer";
import { actions as stmtActions } from "../statementInsights/statementInsights.reducer";
import {
  ExecutionInsightsRequest,
  getTxnInsightEvents,
} from "src/api/insightsApi";
import { PayloadAction } from "@reduxjs/toolkit";

export function* refreshTransactionInsightsSaga(
  action?: PayloadAction<ExecutionInsightsRequest>,
) {
  yield put(actions.request(action.payload));
  yield put(stmtActions.request(action.payload));
}

export function* requestTransactionInsightsSaga(
  action?: PayloadAction<ExecutionInsightsRequest>,
): any {
  try {
    let result: any;
    if (action) {
      result = yield call(getTxnInsightEvents, action.payload);
    } else {
      result = yield call(getTxnInsightEvents);
    }
    yield put(actions.received(result));
  } catch (e) {
    yield put(actions.failed(e));
  }
}

export function* transactionInsightsSaga() {
  yield all([
    takeEvery(actions.refresh, refreshTransactionInsightsSaga),
    takeEvery(actions.request, requestTransactionInsightsSaga),
  ]);
}
