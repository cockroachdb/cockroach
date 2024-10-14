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

import { getClusterLocksState } from "src/api/clusterLocksApi";

import { maybeError } from "../../util";

import { actions } from "./clusterLocks.reducer";

export function* requestClusterLocksSaga(): any {
  try {
    const result = yield call(getClusterLocksState);
    yield put(actions.received(result));
  } catch (e) {
    yield put(actions.failed(maybeError(e)));
  }
}

export function* clusterLocksSaga(): Generator<AllEffect<ForkEffect>> {
  yield all([takeLatest(actions.request, requestClusterLocksSaga)]);
}
