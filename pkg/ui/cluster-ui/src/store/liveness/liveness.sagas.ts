// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { all, call, put, delay, takeLatest } from "redux-saga/effects";
import { getLiveness } from "src/api/livenessApi";
import { actions } from "./liveness.reducer";

import { CACHE_INVALIDATION_PERIOD, throttleWithReset } from "src/store/utils";
import { rootActions } from "../reducers";

export function* refreshLivenessSaga() {
  yield put(actions.request());
}

export function* requestLivenessSaga() {
  try {
    const result = yield call(getLiveness);
    yield put(actions.received(result));
  } catch (e) {
    yield put(actions.failed(e));
  }
}

export function* receivedLivenessSaga(delayMs: number) {
  yield delay(delayMs);
  yield put(actions.invalidated());
}

export function* livenessSaga(
  cacheInvalidationPeriod: number = CACHE_INVALIDATION_PERIOD,
) {
  yield all([
    throttleWithReset(
      cacheInvalidationPeriod,
      actions.refresh,
      [actions.invalidated, actions.failed, rootActions.resetState],
      refreshLivenessSaga,
    ),
    takeLatest(actions.request, requestLivenessSaga),
    takeLatest(actions.received, receivedLivenessSaga, cacheInvalidationPeriod),
  ]);
}
