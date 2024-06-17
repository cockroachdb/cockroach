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
