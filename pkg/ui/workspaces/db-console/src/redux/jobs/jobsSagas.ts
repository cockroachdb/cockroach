// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { api as clusterUiApi } from "@cockroachlabs/cluster-ui";
import { PayloadAction } from "@reduxjs/toolkit";
import { all, call, put, takeEvery } from "redux-saga/effects";

import { refreshListExecutionDetailFiles } from "oss/src/redux/apiReducers";

import {
  COLLECT_EXECUTION_DETAILS,
  collectExecutionDetailsCompleteAction,
  collectExecutionDetailsFailedAction,
} from "./jobsActions";

export function* collectExecutionDetailsSaga(
  action: PayloadAction<clusterUiApi.CollectExecutionDetailsRequest>,
) {
  try {
    yield call(clusterUiApi.collectExecutionDetails, action.payload);
    yield put(collectExecutionDetailsCompleteAction());
    yield put(refreshListExecutionDetailFiles() as any);
  } catch (e) {
    yield put(collectExecutionDetailsFailedAction());
  }
}

export function* jobsSaga() {
  yield all([
    takeEvery(COLLECT_EXECUTION_DETAILS, collectExecutionDetailsSaga),
  ]);
}
