// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { all, call, delay, put, takeLatest } from "redux-saga/effects";

import { actions } from "./job.reducer";
import { getJob, JobRequest } from "src/api/jobsApi";
import { CACHE_INVALIDATION_PERIOD, throttleWithReset } from "../utils";
import { rootActions } from "../reducers";
import { PayloadAction } from "@reduxjs/toolkit";

export function* refreshJobSaga(action: PayloadAction<JobRequest>) {
  yield put(actions.request(action.payload));
}

export function* requestJobSaga(action: PayloadAction<JobRequest>): any {
  try {
    const result = yield call(getJob, action.payload);
    yield put(actions.received(result));
  } catch (e) {
    yield put(actions.failed(e));
  }
}

export function* receivedJobSaga(delayMs: number) {
  yield delay(delayMs);
  yield put(actions.invalidated());
}

export function* jobSaga(
  cacheInvalidationPeriod: number = CACHE_INVALIDATION_PERIOD,
) {
  yield all([
    throttleWithReset(
      cacheInvalidationPeriod,
      actions.refresh,
      [actions.invalidated, rootActions.resetState],
      refreshJobSaga,
    ),
    takeLatest(actions.request, requestJobSaga),
    takeLatest(actions.received, receivedJobSaga, cacheInvalidationPeriod),
  ]);
}
