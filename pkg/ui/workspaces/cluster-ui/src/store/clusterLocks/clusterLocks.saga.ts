// Copyright 2022 The Cockroach Authors.
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
