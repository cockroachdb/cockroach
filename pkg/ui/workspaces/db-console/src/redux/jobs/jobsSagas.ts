// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { all, call, put, takeEvery } from "redux-saga/effects";
import { PayloadAction } from "src/interfaces/action";

import {
  createJobProfilerBundleCompleteAction,
  createJobProfilerBundleFailedAction,
} from "./jobsActions";
import {
  invalidateJobProfilerBundles,
  refreshJobProfilerBundles,
} from "src/redux/apiReducers";
import { api as clusterUiApi } from "@cockroachlabs/cluster-ui";
import { CREATE_JOB_PROFILER_BUNDLE } from "src/redux/jobs/jobsActions";

export function* createJobProfilerBundleSaga(
  action: PayloadAction<clusterUiApi.InsertJobProfilerBundleRequest>,
) {
  try {
    yield call(clusterUiApi.createJobProfilerBundle, action.payload);
    yield put(createJobProfilerBundleCompleteAction());
    yield put(invalidateJobProfilerBundles());
    // PUT expects action with `type` field which isn't defined in `refresh` ThunkAction interface
    yield put(refreshJobProfilerBundles() as any);
  } catch (e) {
    yield put(createJobProfilerBundleFailedAction());
  }
}

export function* jobsSaga() {
  yield all([
    takeEvery(CREATE_JOB_PROFILER_BUNDLE, createJobProfilerBundleSaga),
  ]);
}
