// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { all, call, put, delay, takeLatest } from "redux-saga/effects";

import { getLiveness } from "src/api/livenessApi";
import { CACHE_INVALIDATION_PERIOD, throttleWithReset } from "src/store/utils";

import { maybeError } from "../../util";
import { rootActions } from "../rootActions";

import { actions } from "./liveness.reducer";

export function* refreshLivenessSaga() {
  yield put(actions.request());
}

export function* requestLivenessSaga(): any {
  try {
    const result = yield call(getLiveness);
    yield put(actions.received(result));
  } catch (e) {
    yield put(actions.failed(maybeError(e)));
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
      [actions.invalidated, rootActions.resetState],
      refreshLivenessSaga,
    ),
    takeLatest(actions.request, requestLivenessSaga),
    takeLatest(actions.received, receivedLivenessSaga, cacheInvalidationPeriod),
  ]);
}
