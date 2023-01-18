// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { PayloadAction } from "@reduxjs/toolkit";
import { all, call, put, takeLatest, takeEvery } from "redux-saga/effects";
import {
  getCombinedStatements,
  StatementsRequest,
} from "src/api/statementsApi";
import { resetSQLStats } from "src/api/sqlStatsApi";
import { actions as localStorageActions } from "src/store/localStorage";
import {
  actions as sqlStatsActions,
  UpdateTimeScalePayload,
} from "./sqlStats.reducer";
import { actions as sqlDetailsStatsActions } from "../statementDetails/statementDetails.reducer";
import { actions as stmtInsightActions } from "../insights/statementInsights";
import { actions as txnInsightActions } from "../insights/transactionInsights";

export function* refreshSQLStatsSaga(action: PayloadAction<StatementsRequest>) {
  yield put(sqlStatsActions.request(action.payload));
}

export function* requestSQLStatsSaga(
  action: PayloadAction<StatementsRequest>,
): any {
  try {
    const result = yield call(getCombinedStatements, action.payload);
    yield put(sqlStatsActions.received(result));
  } catch (e) {
    yield put(sqlStatsActions.failed(e));
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
  yield put(sqlStatsActions.invalidated());
  yield put(stmtInsightActions.invalidated());
  yield put(txnInsightActions.invalidated());
}

export function* resetSQLStatsSaga(action: PayloadAction<StatementsRequest>) {
  try {
    yield call(resetSQLStats);
    yield put(sqlDetailsStatsActions.invalidateAll());
    yield put(sqlStatsActions.invalidated());
    yield put(sqlStatsActions.refresh(action.payload));
  } catch (e) {
    yield put(sqlStatsActions.failed(e));
  }
}

export function* sqlStatsSaga() {
  yield all([
    takeLatest(sqlStatsActions.refresh, refreshSQLStatsSaga),
    takeLatest(sqlStatsActions.request, requestSQLStatsSaga),
    takeLatest(sqlStatsActions.updateTimeScale, updateSQLStatsTimeScaleSaga),
    takeEvery(sqlStatsActions.reset, resetSQLStatsSaga),
  ]);
}
