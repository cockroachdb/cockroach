// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { all, call, put, takeLatest } from "redux-saga/effects";

import { actions } from "./job.reducer";
import { getJob, JobRequest, JobResponseWithKey } from "src/api/jobsApi";
import { PayloadAction } from "@reduxjs/toolkit";
import { ErrorWithKey } from "../../api";

export function* refreshJobSaga(action: PayloadAction<JobRequest>) {
  yield put(actions.request(action.payload));
}

export function* requestJobSaga(action: PayloadAction<JobRequest>): any {
  const key = action.payload.job_id.toString();
  try {
    const result = yield call(getJob, action.payload);
    const resultWithKey: JobResponseWithKey = {
      key: key,
      jobResponse: result,
    };
    yield put(actions.received(resultWithKey));
  } catch (e) {
    const err: ErrorWithKey = {
      err: e,
      key,
    };
    yield put(actions.failed(err));
  }
}
export function* jobSaga() {
  yield all([
    takeLatest(actions.refresh, refreshJobSaga),
    takeLatest(actions.request, requestJobSaga),
  ]);
}
