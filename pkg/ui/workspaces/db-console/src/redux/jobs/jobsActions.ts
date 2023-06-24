// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Action } from "redux";
import { PayloadAction } from "src/interfaces/action";
import { api as clusterUiApi } from "@cockroachlabs/cluster-ui";

export const CREATE_JOB_PROFILER_BUNDLE =
  "cockroachui/jobs/CREATE_JOB_PROFILER_BUNDLE";
export const CREATE_JOB_PROFILER_BUNDLE_COMPLETE =
  "cockroachui/jobs/CREATE_JOB_PROFILER_BUNDLE_COMPLETE";
export const CREATE_JOB_PROFILER_BUNDLE_FAILED =
  "cockroachui/jobs/CREATE_JOB_PROFILER_BUNDLE_FAILED";

export function createJobProfilerBundleAction(
  insertJobProfilerBundleRequest: clusterUiApi.InsertJobProfilerBundleRequest,
): PayloadAction<clusterUiApi.InsertJobProfilerBundleRequest> {
  return {
    type: CREATE_JOB_PROFILER_BUNDLE,
    payload: insertJobProfilerBundleRequest,
  };
}

export function createJobProfilerBundleCompleteAction(): Action {
  return {
    type: CREATE_JOB_PROFILER_BUNDLE_COMPLETE,
  };
}

export function createJobProfilerBundleFailedAction(): Action {
  return {
    type: CREATE_JOB_PROFILER_BUNDLE_FAILED,
  };
}
