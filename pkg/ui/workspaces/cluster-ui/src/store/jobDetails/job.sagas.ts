// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { PayloadAction } from "@reduxjs/toolkit";
import { all, call, put, takeLatest } from "redux-saga/effects";

import { getJob, JobRequest, JobResponseWithKey } from "src/api/jobsApi";

import { ErrorWithKey } from "../../api";
import { maybeError } from "../../util";

import { actions } from "./job.reducer";

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
      err: maybeError(e),
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
