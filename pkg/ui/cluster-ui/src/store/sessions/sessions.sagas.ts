// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { all, call, delay, put, takeLatest } from "redux-saga/effects";

import { actions } from "./sessions.reducer";
import { getSessions } from "src/api/sessionsApi";
import { CACHE_INVALIDATION_PERIOD, throttleWithReset } from "../utils";
import { rootActions } from "../reducers";

export function* refreshSessionsSaga() {
  yield put(actions.request());
}

export function* requestSessionsSaga() {
  try {
    const result = yield call(getSessions);
    yield put(actions.received(result));
  } catch (e) {
    yield put(actions.failed(e));
  }
}

export function* receivedStatementsSaga(delayMs: number) {
  yield delay(delayMs);
  yield put(actions.invalidated());
}

export function* sessionsSaga(
  cacheInvalidationPeriod: number = CACHE_INVALIDATION_PERIOD,
) {
  yield all([
    throttleWithReset(
      cacheInvalidationPeriod,
      actions.refresh,
      [actions.invalidated, actions.failed, rootActions.resetState],
      refreshSessionsSaga,
    ),
    takeLatest(actions.request, requestSessionsSaga),
    takeLatest(
      actions.received,
      receivedStatementsSaga,
      cacheInvalidationPeriod,
    ),
  ]);
}
