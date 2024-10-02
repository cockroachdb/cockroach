// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { PayloadAction } from "@reduxjs/toolkit";
import { all, call, put, takeLatest } from "redux-saga/effects";

import { getTxnInsightsApi, TxnInsightsRequest } from "src/api/txnInsightsApi";

import { maybeError } from "../../../util";
import { actions as txnActions } from "../transactionInsights/transactionInsights.reducer";

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
    yield put(txnActions.failed(maybeError(e)));
  }
}

export function* transactionInsightsSaga() {
  yield all([
    takeLatest(txnActions.refresh, refreshTransactionInsightsSaga),
    takeLatest(txnActions.request, requestTransactionInsightsSaga),
  ]);
}
