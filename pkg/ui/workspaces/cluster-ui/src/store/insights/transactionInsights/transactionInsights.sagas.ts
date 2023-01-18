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

import { actions } from "./transactionInsights.reducer";
import { PayloadAction } from "@reduxjs/toolkit";
import {
  getTxnInsightEvents,
  ExecutionInsightsRequest,
} from "src/api/txnInsightsApi";

export function* refreshTransactionInsightsSaga(
  action?: PayloadAction<ExecutionInsightsRequest>,
) {
  yield put(actions.request(action?.payload));
}

export function* requestTransactionInsightsSaga(
  action?: PayloadAction<ExecutionInsightsRequest>,
): any {
  try {
    const result = yield call(getTxnInsightEvents, action?.payload);
    yield put(actions.received(result));
  } catch (e) {
    yield put(actions.failed(e));
  }
}

export function* transactionInsightsSaga() {
  yield all([
    takeLatest(actions.refresh, refreshTransactionInsightsSaga),
    takeLatest(actions.request, requestTransactionInsightsSaga),
  ]);
}
