// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
import { actions as txnStatsActions } from "../transactionStats";
import { actions as sqlDetailsStatsActions } from "../statementDetails/statementDetails.reducer";

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
    localStorageActions.updateTimeScale({
      value: ts,
    }),
  );
}

export function* resetSQLStatsSaga() {
  try {
    yield call(resetSQLStats);
    yield all([
      put(sqlDetailsStatsActions.invalidateAll()),
      put(sqlStatsActions.invalidated()),
      put(txnStatsActions.invalidated()),
    ]);
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
