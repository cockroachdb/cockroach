// Copyright 2021 The Cockroach Authors.
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
  put,
  takeLatest,
  AllEffect,
  PutEffect,
  SelectEffect,
  select,
} from "redux-saga/effects";

import { actions } from "./sessions.reducer";
import { actions as clusterLockActions } from "../clusterLocks/clusterLocks.reducer";
import { getSessions } from "src/api/sessionsApi";
import { selectIsTenant } from "../uiConfig";

export function* refreshSessionsAndClusterLocksSaga(): Generator<
  AllEffect<PutEffect> | SelectEffect | PutEffect
> {
  const isTenant = yield select(selectIsTenant);
  if (isTenant) {
    yield put(actions.request());
    return;
  }
  yield all([put(actions.request()), put(clusterLockActions.request())]);
}

export function* requestSessionsSaga(): any {
  try {
    const result = yield call(getSessions);
    yield put(actions.received(result));
  } catch (e) {
    yield put(actions.failed(e));
  }
}

export function* sessionsSaga() {
  yield all([
    takeLatest(actions.refresh, refreshSessionsAndClusterLocksSaga),
    takeLatest(actions.request, requestSessionsSaga),
  ]);
}
