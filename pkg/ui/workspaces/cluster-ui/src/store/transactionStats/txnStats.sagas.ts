// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { PayloadAction } from "@reduxjs/toolkit";
import { all, call, put, takeLatest } from "redux-saga/effects";
import {
  getFlushedTxnStatsApi,
  StatementsRequest,
} from "src/api/statementsApi";
import { actions as txnStatsActions } from "./txnStats.reducer";

export function* refreshTxnStatsSaga(
  action: PayloadAction<StatementsRequest>,
): any {
  yield put(txnStatsActions.request(action.payload));
}

export function* requestTxnStatsSaga(
  action: PayloadAction<StatementsRequest>,
): any {
  try {
    const result = yield call(getFlushedTxnStatsApi, action.payload);
    yield put(txnStatsActions.received(result));
  } catch (e) {
    yield put(txnStatsActions.failed(e));
  }
}

export function* txnStatsSaga(): any {
  yield all([
    takeLatest(txnStatsActions.refresh, refreshTxnStatsSaga),
    takeLatest(txnStatsActions.request, requestTxnStatsSaga),
  ]);
}
