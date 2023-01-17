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

import { actions as txnActions } from "../transactionInsights/transactionInsights.reducer";
import { getTxnInsightsApi, TxnInsightsRequest } from "src/api/txnInsightsApi";
import { PayloadAction } from "@reduxjs/toolkit";

export function* refreshTransactionInsightsSaga(
  action?: PayloadAction<TxnInsightsRequest>,
) {
  yield put(txnActions.request(action?.payload));
}

export function* requestTransactionInsightsSaga(
  action?: PayloadAction<TxnInsightsRequest>,
): any {
  try {
    const result = yield call(getTxnInsightsApi, action?.payload);
    yield put(txnActions.received(result));
  } catch (e) {
    yield put(txnActions.failed(e));
  }
}

export function* transactionInsightsSaga() {
  yield all([
    takeLatest(txnActions.refresh, refreshTransactionInsightsSaga),
    takeLatest(txnActions.request, requestTransactionInsightsSaga),
  ]);
}
