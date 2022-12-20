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

import { actions } from "./statementInsights.reducer";
import { actions as txnInsightActions } from "../transactionInsights";
import { StmtInsightsReq, getStmtInsightsApi } from "src/api/stmtInsightsApi";
import { PayloadAction } from "@reduxjs/toolkit";
import {
  UpdateTimeScalePayload,
  actions as sqlStatsActions,
} from "../../sqlStats";
import { actions as localStorageActions } from "../../localStorage";
import { executionInsightsRequestFromTimeScale } from "../../../insights";

export function* refreshStatementInsightsSaga(
  action?: PayloadAction<StmtInsightsReq>,
) {
  yield put(actions.request(action?.payload));
}

export function* requestStatementInsightsSaga(
  action?: PayloadAction<StmtInsightsReq>,
): any {
  try {
    const result = yield call(getStmtInsightsApi, action?.payload);
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
  yield put(txnInsightActions.invalidated());
  yield put(sqlStatsActions.invalidated());
  yield put(actions.refresh(req));
}

export function* statementInsightsSaga() {
  yield all([
    takeLatest(actions.refresh, refreshStatementInsightsSaga),
    takeLatest(actions.request, requestStatementInsightsSaga),
    takeLatest(
      [actions.updateTimeScale, txnInsightActions.updateTimeScale],
      updateSQLStatsTimeScaleSaga,
    ),
  ]);
}
