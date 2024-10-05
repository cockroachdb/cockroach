// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { PayloadAction } from "@reduxjs/toolkit";
import { actions } from "./jobProfiler.reducer";
import { call, put, all, takeEvery } from "redux-saga/effects";
import {
  ListJobProfilerExecutionDetailsRequest,
  collectExecutionDetails,
  listExecutionDetailFiles,
} from "src/api";

export function* refreshJobProfilerSaga(
  action: PayloadAction<ListJobProfilerExecutionDetailsRequest>,
) {
  yield put(actions.request(action.payload));
}

export function* requestJobProfilerSaga(
  action: PayloadAction<ListJobProfilerExecutionDetailsRequest>,
): any {
  try {
    const result = yield call(listExecutionDetailFiles, action.payload);
    yield put(actions.received(result));
  } catch (e) {
    yield put(actions.failed(e));
  }
}

export function* collectExecutionDetailsSaga(
  action: ReturnType<typeof actions.collectExecutionDetails>,
) {
  try {
    yield call(collectExecutionDetails, action.payload);
    yield put(actions.collectExecutionDetailsCompleted());
    // request execution details to reflect changed state for newly
    // requested statement.
    yield put(actions.request());
  } catch (e) {
    yield put(actions.collectExecutionDetailsFailed(e));
  }
}

export function* jobProfilerSaga() {
  yield all([
    takeEvery(actions.refresh, refreshJobProfilerSaga),
    takeEvery(actions.request, requestJobProfilerSaga),
    takeEvery(actions.collectExecutionDetails, collectExecutionDetailsSaga),
  ]);
}
