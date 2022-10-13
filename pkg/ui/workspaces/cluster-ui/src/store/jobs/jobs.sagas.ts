// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { all, call, put, takeEvery } from "redux-saga/effects";

import { actions } from "./jobs.reducer";
import { getJobs, JobsRequest } from "src/api/jobsApi";
import { PayloadAction } from "@reduxjs/toolkit";

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

export function* jobsSaga() {
  yield all([
    takeEvery(actions.refresh, refreshJobsSaga),
    takeEvery(actions.request, requestJobsSaga),
  ]);
}
