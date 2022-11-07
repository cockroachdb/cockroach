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

import { actions } from "./transactionInsightCounts.reducer";
import {
  TxnInsightsRequest,
  getTransactionInsightCount,
} from "src/api/txnInsightsApi";
import { PayloadAction } from "@reduxjs/toolkit";

export function* refreshTransactionInsightCountsSaga(
  action: PayloadAction<TxnInsightsRequest>,
) {
  yield put(actions.request(action.payload));
}

export function* requestTransactionInsightCountsSaga(
  action: PayloadAction<TxnInsightsRequest>,
): any {
  try {
    const result = yield call(getTransactionInsightCount, action.payload);
    yield put(actions.received(result));
  } catch (e) {
    yield put(actions.failed(e));
  }
}

export function* transactionInsightCountsSaga() {
  yield all([
    takeLatest(actions.refresh, refreshTransactionInsightCountsSaga),
    takeLatest(actions.request, requestTransactionInsightCountsSaga),
  ]);
}
