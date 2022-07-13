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
  select,
  SelectEffect,
  takeLatest,
  PutEffect,
} from "redux-saga/effects";

import { actions } from "./sessions.reducer";
import { getSessions } from "src/api/sessionsApi";
import { throttleWithReset } from "../utils";
import { rootActions } from "../reducers";

export function* refreshSessionsAndClusterLocksSaga(): Generator<
  PutEffect | SelectEffect
> {
  yield put(actions.request());

  // TODO (xzhang) request clusterLocks info here. This is currently not available on CC.
}

export function* requestSessionsSaga(): any {
  try {
    const result = yield call(getSessions);
    yield put(actions.received(result));
  } catch (e) {
    yield put(actions.failed(e));
  }
}

export function* sessionsSaga(cacheInvalidationPeriod: number = 5 * 1000) {
  yield all([
    throttleWithReset(
      cacheInvalidationPeriod,
      actions.refresh,
      [actions.invalidated, rootActions.resetState],
      refreshSessionsAndClusterLocksSaga,
    ),
    takeLatest(actions.request, requestSessionsSaga),
  ]);
}
