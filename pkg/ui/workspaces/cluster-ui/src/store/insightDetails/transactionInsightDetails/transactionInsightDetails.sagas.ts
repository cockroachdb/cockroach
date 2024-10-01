// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { PayloadAction } from "@reduxjs/toolkit";
import { all, call, put, takeLatest, takeEvery } from "redux-saga/effects";

import { ErrorWithKey, SqlApiResponse } from "src/api";

import {
  getTxnInsightDetailsApi,
  TxnInsightDetailsRequest,
  TxnInsightDetailsResponse,
} from "../../../api/txnInsightDetailsApi";
import { maybeError } from "../../../util";

import { actions } from "./transactionInsightDetails.reducer";

export function* refreshTransactionInsightDetailsSaga(
  action: PayloadAction<TxnInsightDetailsRequest>,
) {
  yield put(actions.request(action.payload));
}

export function* requestTransactionInsightDetailsSaga(
  action: PayloadAction<TxnInsightDetailsRequest>,
): any {
  try {
    const result = yield call(getTxnInsightDetailsApi, action.payload);
    yield put(actions.received(result));
  } catch (e) {
    const err: ErrorWithKey = {
      err: maybeError(e),
      key: action.payload.txnExecutionID,
    };
    yield put(actions.failed(err));
  }
}

const CACHE_INVALIDATION_PERIOD = 5 * 60 * 1000; // 5 minutes in ms

const timeoutsByExecID = new Map<string, NodeJS.Timeout>();

export function receivedTxnInsightsDetailsSaga(
  action: PayloadAction<SqlApiResponse<TxnInsightDetailsResponse>>,
) {
  const execID = action.payload.results.txnExecutionID;
  clearTimeout(timeoutsByExecID.get(execID));
  const id = setTimeout(() => {
    actions.invalidated({ key: execID });
    timeoutsByExecID.delete(execID);
  }, CACHE_INVALIDATION_PERIOD);
  timeoutsByExecID.set(execID, id);
}

export function* transactionInsightDetailsSaga() {
  yield all([
    takeEvery(actions.refresh, refreshTransactionInsightDetailsSaga),
    takeLatest(actions.request, requestTransactionInsightDetailsSaga),
    takeLatest(actions.received, receivedTxnInsightsDetailsSaga),
  ]);
}
