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
import {
  RESET_SQL_STATS,
  resetSQLStatsCompleteAction,
  resetSQLStatsFailedAction,
} from "./sqlStatsActions";
import { invalidateStatements, refreshStatements } from "src/redux/apiReducers";

import ResetSQLStatsRequest = cockroach.server.serverpb.ResetSQLStatsRequest;

export function* resetSQLStatsSaga() {
  const resetSQLStatsRequest = new ResetSQLStatsRequest();
  try {
    yield call(resetSQLStats, resetSQLStatsRequest);
    yield put(resetSQLStatsCompleteAction());
    yield put(invalidateStatements());
    yield put(refreshStatements() as any);
  } catch (e) {
    yield put(resetSQLStatsFailedAction());
  }
}

export function* sqlStatsSaga() {
  yield all([takeEvery(RESET_SQL_STATS, resetSQLStatsSaga)]);
}
