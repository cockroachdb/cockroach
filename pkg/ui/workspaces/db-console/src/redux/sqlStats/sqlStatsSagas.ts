// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { cockroach } from "src/js/protos";
import { resetSQLStats } from "src/util/api";
import { all, call, put, takeEvery } from "redux-saga/effects";
import { RESET_SQL_STATS, resetSQLStatsFailedAction } from "./sqlStatsActions";
import {
  invalidateAllStatementDetails,
  invalidateStatements,
} from "src/redux/apiReducers";

import ResetSQLStatsRequest = cockroach.server.serverpb.ResetSQLStatsRequest;

export function* resetSQLStatsSaga() {
  const resetSQLStatsRequest = new ResetSQLStatsRequest({
    // reset_persisted_stats is set to true in order to clear both
    // in-memory stats as well as persisted stats.
    reset_persisted_stats: true,
  });
  try {
    yield call(resetSQLStats, resetSQLStatsRequest);
    yield all([
      put(invalidateStatements()),
      put(invalidateAllStatementDetails()),
    ]);
  } catch (e) {
    yield put(resetSQLStatsFailedAction());
  }
}

export function* sqlStatsSaga() {
  yield all([takeEvery(RESET_SQL_STATS, resetSQLStatsSaga)]);
}
