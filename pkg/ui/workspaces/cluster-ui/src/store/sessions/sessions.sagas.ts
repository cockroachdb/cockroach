// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { PayloadAction } from "@reduxjs/toolkit";
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

import { getSessions, SessionsRequest } from "src/api/sessionsApi";

import { maybeError } from "../../util";
import { actions as clusterLockActions } from "../clusterLocks/clusterLocks.reducer";
import { selectIsTenant } from "../uiConfig";

import { actions } from "./sessions.reducer";

export function* refreshSessionsAndClusterLocksSaga(
  action: PayloadAction<SessionsRequest | undefined>,
): Generator<AllEffect<PutEffect> | SelectEffect | PutEffect> {
  const isTenant = yield select(selectIsTenant);
  if (isTenant) {
    yield put(actions.request(action?.payload));
    return;
  }
  yield all([
    put(actions.request(action?.payload)),
    put(clusterLockActions.request()),
  ]);
}

export function* requestSessionsSaga(
  action: PayloadAction<SessionsRequest | undefined>,
): any {
  try {
    const result = yield call(getSessions, action?.payload);
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
