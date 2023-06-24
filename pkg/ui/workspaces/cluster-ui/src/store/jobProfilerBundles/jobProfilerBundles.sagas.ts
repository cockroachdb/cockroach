// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import {
  all,
  call,
  delay,
  put,
  takeEvery,
  takeLatest,
} from "redux-saga/effects";
import { actions } from "./jobProfilerBundles.reducer";
import { rootActions } from "../reducers";
import { CACHE_INVALIDATION_PERIOD, throttleWithReset } from "../utils";
import {
  createJobProfilerBundle,
  getJobProfilerBundles,
} from "src/api/jobProfilerBundleApi";

export function* createJobProfilerBundleSaga(
  action: ReturnType<typeof actions.createBundle>,
) {
  try {
    yield call(createJobProfilerBundle, action.payload);
    yield put(actions.createBundleCompleted());
    // request diagnostics reports to reflect changed state for newly
    // requested statement.
    yield put(actions.request());
  } catch (e) {
    yield put(actions.createBundleFailed(e));
  }
}

export function* refreshJobProfilerBundlesSaga() {
  yield put(actions.request());
}

export function* requestJobProfilerBundlesSaga(): any {
  try {
    const response = yield call(getJobProfilerBundles);
    yield put(actions.received(response));
  } catch (e) {
    yield put(actions.failed(e));
  }
}

export function* receivedJobProfilerBundlesSaga(delayMs: number) {
  yield delay(delayMs);
  yield put(actions.invalidated());
}

export function* jobProfilerBundlesSaga(
  delayMs: number = CACHE_INVALIDATION_PERIOD,
) {
  yield all([
    throttleWithReset(
      delayMs,
      actions.refresh,
      [actions.invalidated, rootActions.resetState],
      refreshJobProfilerBundlesSaga,
    ),
    takeLatest(actions.request, requestJobProfilerBundlesSaga),
    takeLatest(actions.received, receivedJobProfilerBundlesSaga, delayMs),
    takeEvery(actions.createBundle, createJobProfilerBundleSaga),
  ]);
}
