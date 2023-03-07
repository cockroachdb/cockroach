// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { AnyAction } from "redux";
import { all, call, takeEvery, takeLatest, put } from "redux-saga/effects";
import { actions } from "./localStorage.reducer";
import { actions as sqlStatsActions } from "src/store/sqlStats";
import { actions as stmtInsightActions } from "src/store/insights/statementInsights";
import { actions as txnInsightActions } from "src/store/insights/transactionInsights";
import { actions as txnStatsActions } from "src/store/transactionStats";

export function* updateLocalStorageItemSaga(action: AnyAction) {
  const { key, value } = action.payload;
  yield call(
    { context: localStorage, fn: localStorage.setItem },
    key,
    JSON.stringify(value),
  );
}

export function* updateTimeScale() {
  yield all([
    put(sqlStatsActions.invalidated()),
    put(stmtInsightActions.invalidated()),
    put(txnInsightActions.invalidated()),
    put(txnStatsActions.invalidated()),
  ]);
}

export function* localStorageSaga() {
  yield all([
    takeEvery(actions.update, updateLocalStorageItemSaga),
    takeLatest(actions.updateTimeScale, updateLocalStorageItemSaga),
  ]);
}
