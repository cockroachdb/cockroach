// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { all, call, put, takeEvery } from "redux-saga/effects";
import { resetSQLStats } from "src/api/sqlStatsApi";
import { actions as statementActions } from "src/store/statements";
import { actions as sqlStatsActions } from "./sqlStats.reducer";

export function* resetSQLStatsSaga() {
  try {
    const response = yield call(resetSQLStats);
    yield put(sqlStatsActions.received(response));
    yield put(statementActions.invalidated());
    yield put(statementActions.refresh());
  } catch (e) {
    yield put(sqlStatsActions.failed(e));
  }
}

export function* sqlStatsSaga() {
  yield all([takeEvery(sqlStatsActions.request, resetSQLStatsSaga)]);
}
