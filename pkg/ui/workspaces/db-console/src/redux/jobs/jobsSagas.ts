// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { PayloadAction } from "@reduxjs/toolkit";
import { refreshListExecutionDetailFiles } from "oss/src/redux/apiReducers";
import { all, call, put, takeEvery } from "redux-saga/effects";
import { api as clusterUiApi } from "@cockroachlabs/cluster-ui";
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
