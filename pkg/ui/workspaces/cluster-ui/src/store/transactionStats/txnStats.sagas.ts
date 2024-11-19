// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { PayloadAction } from "@reduxjs/toolkit";
import { all, call, put, takeLatest } from "redux-saga/effects";

import {
  getFlushedTxnStatsApi,
  StatementsRequest,
} from "src/api/statementsApi";

import { maybeError } from "../../util";

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
    yield put(txnStatsActions.failed(maybeError(e)));
  }
}

export function* txnStatsSaga(): any {
  yield all([
    takeLatest(txnStatsActions.refresh, refreshTxnStatsSaga),
    takeLatest(txnStatsActions.request, requestTxnStatsSaga),
  ]);
}
