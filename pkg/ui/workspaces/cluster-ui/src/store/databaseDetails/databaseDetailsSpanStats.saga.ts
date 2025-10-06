// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { PayloadAction } from "@reduxjs/toolkit";
import { all, call, put, takeEvery } from "redux-saga/effects";

import {
  DatabaseDetailsSpanStatsReqParams,
  ErrorWithKey,
  getDatabaseDetailsSpanStats,
} from "src/api";

import { maybeError } from "../../util";

import { databaseDetailsSpanStatsReducer } from "./databaseDetails.reducer";

const actions = databaseDetailsSpanStatsReducer.actions;
export function* refreshDatabaseDetailsSpanStatsSaga(
  action: PayloadAction<DatabaseDetailsSpanStatsReqParams>,
) {
  yield put(actions.request(action.payload));
}

export function* requestDatabaseDetailsSpanStatsSaga(
  action: PayloadAction<DatabaseDetailsSpanStatsReqParams>,
): any {
  try {
    const result = yield call(getDatabaseDetailsSpanStats, action.payload);
    yield put(
      actions.received({
        key: action.payload.database,
        response: result,
      }),
    );
  } catch (e) {
    const err: ErrorWithKey = {
      err: maybeError(e),
      key: action.payload.database,
    };
    yield put(actions.failed(err));
  }
}

export function* databaseDetailsSpanStatsSaga() {
  yield all([
    takeEvery(actions.refresh, refreshDatabaseDetailsSpanStatsSaga),
    takeEvery(actions.request, requestDatabaseDetailsSpanStatsSaga),
  ]);
}
