// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

import { getSessions } from "src/api/sessionsApi";

import { maybeError } from "../../util";
import { actions as clusterLockActions } from "../clusterLocks/clusterLocks.reducer";
import { selectIsTenant } from "../uiConfig";

import { actions } from "./sessions.reducer";

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
    yield put(actions.failed(maybeError(e)));
  }
}

export function* sessionsSaga() {
  yield all([
    takeLatest(actions.refresh, refreshSessionsAndClusterLocksSaga),
    takeLatest(actions.request, requestSessionsSaga),
  ]);
}
