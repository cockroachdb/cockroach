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
import {
  all,
  call,
  put,
  delay,
  takeLatest,
  takeEvery,
} from "redux-saga/effects";
import Long from "long";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import {
  getStatements,
  getCombinedStatements,
  StatementsRequest,
} from "src/api/statementsApi";
import { resetSQLStats } from "src/api/sqlStatsApi";
import { actions as localStorageActions } from "src/store/localStorage";
import {
  actions as sqlStatsActions,
  UpdateDateRangePayload,
} from "./sqlStats.reducer";
import { rootActions } from "../reducers";

import { CACHE_INVALIDATION_PERIOD, throttleWithReset } from "src/store/utils";

export function* refreshSQLStatsSaga(
  action?: PayloadAction<StatementsRequest>,
) {
  yield put(sqlStatsActions.request(action?.payload));
}

export function* requestSQLStatsSaga(
  action?: PayloadAction<StatementsRequest>,
): any {
  try {
    const result = yield action?.payload?.combined
      ? call(getCombinedStatements, action.payload)
      : call(getStatements);
    yield put(sqlStatsActions.received(result));
  } catch (e) {
    yield put(sqlStatsActions.failed(e));
  }
}

export function* receivedSQLStatsSaga(delayMs: number) {
  yield delay(delayMs);
  yield put(sqlStatsActions.invalidated());
}

export function* updateSQLStatsDateRangeSaga(
  action: PayloadAction<UpdateDateRangePayload>,
) {
  const { start, end } = action.payload;
  yield put(
    // TODO(azhng): do we want to rename this into dataRange/SQLActivity?
    localStorageActions.update({
      key: "dateRange/StatementsPage",
      value: { start, end },
    }),
  );
  yield put(sqlStatsActions.invalidated());
  const req = new cockroach.server.serverpb.StatementsRequest({
    combined: true,
    start: Long.fromNumber(start),
    end: Long.fromNumber(end),
  });
  yield put(sqlStatsActions.refresh(req));
}

export function* resetSQLStatsSaga() {
  try {
    yield call(resetSQLStats);
    yield put(sqlStatsActions.invalidated());
    yield put(sqlStatsActions.refresh());
  } catch (e) {
    yield put(sqlStatsActions.failed(e));
  }
}

export function* sqlStatsSaga(
  cacheInvalidationPeriod: number = CACHE_INVALIDATION_PERIOD,
) {
  yield all([
    throttleWithReset(
      cacheInvalidationPeriod,
      sqlStatsActions.refresh,
      [
        sqlStatsActions.invalidated,
        sqlStatsActions.failed,
        rootActions.resetState,
      ],
      refreshSQLStatsSaga,
    ),
    takeLatest(sqlStatsActions.request, requestSQLStatsSaga),
    takeLatest(
      sqlStatsActions.received,
      receivedSQLStatsSaga,
      cacheInvalidationPeriod,
    ),
    takeLatest(sqlStatsActions.updateDateRange, updateSQLStatsDateRangeSaga),
    takeEvery(sqlStatsActions.reset, resetSQLStatsSaga),
  ]);
}
