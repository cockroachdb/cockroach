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
import { rootActions } from "../reducers";
import { CACHE_INVALIDATION_PERIOD, throttleWithReset } from "src/store/utils";
import { toDateRange } from "../../timeScaleDropdown";

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

export function* receivedSQLStatsSaga(delayMs: number) {
  yield delay(delayMs);
  yield put(sqlStatsActions.invalidated());
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
  const [start, end] = toDateRange(ts);
  const req = new cockroach.server.serverpb.StatementsRequest({
    combined: true,
    start: Long.fromNumber(start.unix()),
    end: Long.fromNumber(end.unix()),
  });
  yield put(sqlStatsActions.refresh(req));
}

export function* resetSQLStatsSaga(action: PayloadAction<StatementsRequest>) {
  try {
    yield call(resetSQLStats);
    yield put(sqlStatsActions.invalidated());
    yield put(sqlDetailsStatsActions.invalidateAll());
    yield put(sqlStatsActions.refresh(action.payload));
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
      [sqlStatsActions.invalidated, rootActions.resetState],
      refreshSQLStatsSaga,
    ),
    takeLatest(sqlStatsActions.request, requestSQLStatsSaga),
    takeLatest(
      sqlStatsActions.received,
      receivedSQLStatsSaga,
      cacheInvalidationPeriod,
    ),
    takeLatest(sqlStatsActions.updateTimeScale, updateSQLStatsTimeScaleSaga),
    takeEvery(sqlStatsActions.reset, resetSQLStatsSaga),
  ]);
}
