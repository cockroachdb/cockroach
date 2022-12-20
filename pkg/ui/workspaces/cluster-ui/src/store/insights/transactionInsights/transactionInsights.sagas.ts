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
import { actions as stmtInsightActions } from "../statementInsights";
import { PayloadAction } from "@reduxjs/toolkit";
import {
  UpdateTimeScalePayload,
  actions as sqlStatsActions,
} from "../../sqlStats";
import { actions as localStorageActions } from "../../localStorage";
import { executionInsightsRequestFromTimeScale } from "../../../insights";
import { getTxnInsightEvents } from "src/api/txnInsightsApi";
import { TxnContentionReq } from "src/api";

export function* refreshTransactionInsightsSaga(
  action?: PayloadAction<TxnContentionReq>,
) {
  yield put(actions.request(action?.payload));
  yield put(stmtInsightActions.request(action.payload));
}

export function* requestTransactionInsightsSaga(
  action?: PayloadAction<TxnContentionReq>,
): any {
  try {
    const result = yield call(getTxnInsightEvents, action?.payload);
    yield put(actions.received(result));
  } catch (e) {
    yield put(actions.failed(e));
  }
}

export function* updateSQLStatsTimeScaleSaga(
  action: PayloadAction<UpdateTimeScalePayload>,
) {
  const { ts } = action.payload;
  yield put(
    localStorageActions.update({
      key: "timeScale/SQLActivity",
      value: ts,
    }),
  );
  const req = executionInsightsRequestFromTimeScale(ts);
  yield put(actions.invalidated());
  yield put(stmtInsightActions.invalidated());
  yield put(sqlStatsActions.invalidated());
  yield put(actions.refresh(req));
}

export function* transactionInsightsSaga() {
  yield all([
    takeLatest(actions.refresh, refreshTransactionInsightsSaga),
    takeLatest(actions.request, requestTransactionInsightsSaga),
    takeLatest(
      [actions.updateTimeScale, stmtInsightActions.updateTimeScale],
      updateSQLStatsTimeScaleSaga,
    ),
  ]);
}
