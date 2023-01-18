// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { all, call, put, takeLatest, takeEvery } from "redux-saga/effects";

import { actions } from "./transactionInsightDetails.reducer";
import {
  getTransactionInsightEventDetailsState,
  TxnContentionInsightDetailsRequest,
} from "src/api/txnInsightsApi";
import { TxnContentionInsightDetails } from "src/insights";
import { PayloadAction } from "@reduxjs/toolkit";
import { ErrorWithKey } from "src/api";
import { actions as stmtInsightActions } from "../../insights/statementInsights";

export function* refreshTransactionInsightDetailsSaga(
  action: PayloadAction<TxnContentionInsightDetailsRequest>,
) {
  yield put(actions.request(action.payload));
  yield put(
    stmtInsightActions.request({
      start: action.payload.start,
      end: action.payload.end,
    }),
  );
}

export function* requestTransactionInsightDetailsSaga(
  action: PayloadAction<TxnContentionInsightDetailsRequest>,
): any {
  try {
    const result = yield call(
      getTransactionInsightEventDetailsState,
      action.payload,
    );
    yield put(actions.received(result));
  } catch (e) {
    const err: ErrorWithKey = {
      err: e,
      key: action.payload.id,
    };
    yield put(actions.failed(err));
  }
}

const CACHE_INVALIDATION_PERIOD = 5 * 60 * 1000; // 5 minutes in ms

const timeoutsByExecID = new Map<string, NodeJS.Timeout>();

export function receivedTxnInsightsDetailsSaga(
  action: PayloadAction<TxnContentionInsightDetails>,
) {
  if (action?.payload?.transactionExecutionID) {
    const execID = action.payload.transactionExecutionID;
    clearTimeout(timeoutsByExecID.get(execID));
    const id = setTimeout(() => {
      actions.invalidated({ key: execID });
      stmtInsightActions.invalidated();
      timeoutsByExecID.delete(execID);
    }, CACHE_INVALIDATION_PERIOD);
    timeoutsByExecID.set(execID, id);
  }
}

export function* transactionInsightDetailsSaga() {
  yield all([
    takeEvery(actions.refresh, refreshTransactionInsightDetailsSaga),
    takeLatest(actions.request, requestTransactionInsightDetailsSaga),
    takeLatest(actions.received, receivedTxnInsightsDetailsSaga),
  ]);
}
