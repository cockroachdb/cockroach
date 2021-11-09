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
import { actions as transactionActions } from "src/store/transactions";
import { actions as sqlStatsActions } from "./sqlStats.reducer";

export function* resetSQLStatsSaga(): any {
  try {
    const response = yield call(resetSQLStats);
    yield put(sqlStatsActions.received(response));

    // We dispatch INVALIDATE actions for both statements and transactions,
    // but we only dispatch one REFRESH action for statements.
    // The responsibility of issuing API call is delegated to the React
    // components since the invalidation of the props will cause the React
    // lifecycle hook to be called.
    yield put(statementActions.invalidated());
    yield put(transactionActions.invalidated());
  } catch (e) {
    yield put(sqlStatsActions.failed(e));
  }
}

export function* sqlStatsSaga() {
  yield all([takeEvery(sqlStatsActions.request, resetSQLStatsSaga)]);
}
