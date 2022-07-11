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

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { actions } from "./jobs.reducer";
import { getJobs, JobsRequest } from "src/api/jobsApi";
import { CACHE_INVALIDATION_PERIOD, throttleWithReset } from "../utils";
import { PayloadAction } from "@reduxjs/toolkit";
import { rootActions } from "../reducers";

export function* refreshJobsSaga(action: PayloadAction<JobsRequest>) {
  yield put(actions.request(action.payload));
}

export function* requestJobsSaga(action: PayloadAction<JobsRequest>): any {
  try {
    const result = yield call(getJobs, action.payload);
    yield put(actions.received(result));
  } catch (e) {
    yield put(actions.failed(e));
  }
}

export function* receivedJobsSaga(delayMs: number) {
  yield delay(delayMs);
  yield put(actions.invalidated());
}

export function* updateFilteredJobsSaga(action: PayloadAction<JobsRequest>) {
  yield put(actions.invalidated());
  const req = new cockroach.server.serverpb.JobsRequest(action.payload);
  yield put(actions.refresh(req));
}

export function* jobsSaga(
  cacheInvalidationPeriod: number = CACHE_INVALIDATION_PERIOD,
) {
  yield all([
    throttleWithReset(
      cacheInvalidationPeriod,
      actions.refresh,
      [actions.invalidated, rootActions.resetState],
      refreshJobsSaga,
    ),
    takeLatest(actions.request, requestJobsSaga),
    takeLatest(actions.received, receivedJobsSaga, cacheInvalidationPeriod),
    takeLatest(actions.updateFilteredJobs, updateFilteredJobsSaga),
  ]);
}
