// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { PayloadAction } from "@reduxjs/toolkit";
import { AnyAction } from "redux";
import { all, call, takeEvery, takeLatest, put } from "redux-saga/effects";

import { actions as stmtInsightActions } from "src/store/insights/statementInsights";
import { actions as txnInsightActions } from "src/store/insights/transactionInsights";
import { actions as sqlStatsActions } from "src/store/sqlStats";
import { actions as txnStatsActions } from "src/store/transactionStats";
import { TimeScale } from "src/timeScaleDropdown";

import {
  actions,
  LocalStorageKeys,
  TypedPayload,
} from "./localStorage.reducer";

export function* updateLocalStorageItemSaga(action: AnyAction) {
  const { key, value } = action.payload;
  yield call(
    { context: localStorage, fn: localStorage.setItem },
    key,
    JSON.stringify(value),
  );
}

export function* updateTimeScale(
  action: PayloadAction<TypedPayload<TimeScale>>,
) {
  yield all([
    put(sqlStatsActions.invalidated()),
    put(stmtInsightActions.invalidated()),
    put(txnInsightActions.invalidated()),
    put(txnStatsActions.invalidated()),
  ]);
  yield call(
    { context: localStorage, fn: localStorage.setItem },
    LocalStorageKeys.GLOBAL_TIME_SCALE,
    JSON.stringify(action.payload?.value),
  );
}

export function* localStorageSaga() {
  yield all([
    takeEvery(actions.update, updateLocalStorageItemSaga),
    takeLatest(actions.updateTimeScale, updateTimeScale),
  ]);
}
