// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  all,
  AllEffect,
  call,
  ForkEffect,
  put,
  takeLatest,
} from "redux-saga/effects";

import { actions } from "./clusterLocks.reducer";
import { getClusterLocksState } from "src/api/clusterLocksApi";

export function* requestClusterLocksSaga(): any {
  try {
    const result = yield call(getClusterLocksState);
    yield put(actions.received(result));
  } catch (e) {
    yield put(actions.failed(e));
  }
}

export function* clusterLocksSaga(): Generator<AllEffect<ForkEffect>> {
  yield all([takeLatest(actions.request, requestClusterLocksSaga)]);
}
