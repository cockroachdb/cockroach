// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { all, call, put, delay, takeLatest, select } from "redux-saga/effects";
import { getCombinedStatements } from "src/api/statementsApi";
import { actions, UpdateDateRangePayload } from "./combinedStatements.reducer";
import { rootActions } from "../reducers";
import { actions as localStorageActions } from "../localStorage";

import { CACHE_INVALIDATION_PERIOD, throttleWithReset } from "src/store/utils";
import { PayloadAction } from "@reduxjs/toolkit";
import { selectLocalStorageDateRange } from "src/statementsPage/statementsPage.selectors";
import { StatementsRequest } from "src/api/statementsApi";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import Long from "long";

export function* refreshCombinedStatementsSaga(
  action: PayloadAction<StatementsRequest>,
) {
  yield put(actions.request(action.payload));
}

export function* requestCombinedStatementsSaga(
  action: PayloadAction<StatementsRequest>,
): any {
  try {
    const result = yield call(getCombinedStatements, action.payload);
    yield put(actions.received(result));
  } catch (e) {
    yield put(actions.failed(e));
  }
}

export function* receivedCombinedStatementsSaga(delayMs: number) {
  yield delay(delayMs);
  yield put(actions.invalidated());
}

export function* updateStatementesDateRangeSaga(
  action: PayloadAction<UpdateDateRangePayload>,
) {
  const { start, end } = action.payload;
  yield put(
    localStorageActions.update({
      key: "dateRange/StatementsPage",
      value: { start, end },
    }),
  );
  yield put(actions.invalidated());
  const req = new cockroach.server.serverpb.StatementsRequest({
    combined: true,
    start: Long.fromNumber(start),
    end: Long.fromNumber(end),
  });
  yield put(actions.refresh(req));
}

export function* combinedStatementsSaga(
  cacheInvalidationPeriod: number = CACHE_INVALIDATION_PERIOD,
) {
  yield all([
    throttleWithReset(
      cacheInvalidationPeriod,
      actions.refresh,
      [actions.invalidated, actions.failed, rootActions.resetState],
      refreshCombinedStatementsSaga,
    ),
    takeLatest(actions.request, requestCombinedStatementsSaga),
    takeLatest(actions.updateDateRange, updateStatementesDateRangeSaga),
    takeLatest(
      actions.received,
      receivedCombinedStatementsSaga,
      cacheInvalidationPeriod,
    ),
  ]);
}
