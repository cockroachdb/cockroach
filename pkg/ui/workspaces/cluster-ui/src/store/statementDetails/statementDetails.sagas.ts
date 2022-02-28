// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { PayloadAction } from "@reduxjs/toolkit";
import { all, call, put, delay, takeLatest } from "redux-saga/effects";
import {
  getStatementDetails,
  StatementDetailsRequest,
} from "src/api/statementsApi";
import { actions as sqlDetailsStatsActions } from "./statementDetails.reducer";
import { rootActions } from "../reducers";
import { CACHE_INVALIDATION_PERIOD, throttleWithReset } from "src/store/utils";

export function* refreshSQLDetailsStatsSaga(
  action?: PayloadAction<StatementDetailsRequest>,
) {
  yield put(sqlDetailsStatsActions.request(action?.payload));
}

export function* requestSQLDetailsStatsSaga(
  action?: PayloadAction<StatementDetailsRequest>,
): any {
  try {
    const result = yield call(getStatementDetails, action?.payload);
    yield put(sqlDetailsStatsActions.received(result));
  } catch (e) {
    yield put(sqlDetailsStatsActions.failed(e));
  }
}

export function* receivedSQLDetailsStatsSaga(delayMs: number) {
  yield delay(delayMs);
  yield put(sqlDetailsStatsActions.invalidated());
}

export function* sqlDetailsStatsSaga(
  cacheInvalidationPeriod: number = CACHE_INVALIDATION_PERIOD,
) {
  yield all([
    throttleWithReset(
      cacheInvalidationPeriod,
      sqlDetailsStatsActions.refresh,
      [sqlDetailsStatsActions.invalidated, rootActions.resetState],
      refreshSQLDetailsStatsSaga,
    ),
    takeLatest(sqlDetailsStatsActions.request, requestSQLDetailsStatsSaga),
    takeLatest(
      sqlDetailsStatsActions.received,
      receivedSQLDetailsStatsSaga,
      cacheInvalidationPeriod,
    ),
  ]);
}
