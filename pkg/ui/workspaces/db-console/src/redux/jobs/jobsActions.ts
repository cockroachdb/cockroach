// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { api as clusterUiApi } from "@cockroachlabs/cluster-ui";
import { Action } from "redux";

import { PayloadAction } from "src/interfaces/action";

export const COLLECT_EXECUTION_DETAILS =
  "cockroachui/jobs/COLLECT_EXECUTION_DETAILS";
export const COLLECT_EXECUTION_DETAILS_COMPLETE =
  "cockroachui/jobs/COLLECT_EXECUTION_DETAILS_COMPLETE";
export const COLLECT_EXECUTION_DETAILS_FAILED =
  "cockroachui/jobs/COLLECT_EXECUTION_DETAILS_FAILED";

export function collectExecutionDetailsAction(
  collectExecutionDetailsRequest: clusterUiApi.CollectExecutionDetailsRequest,
): PayloadAction<clusterUiApi.CollectExecutionDetailsRequest> {
  return {
    type: COLLECT_EXECUTION_DETAILS,
    payload: collectExecutionDetailsRequest,
  };
}

export function collectExecutionDetailsCompleteAction(): Action {
  return {
    type: COLLECT_EXECUTION_DETAILS_COMPLETE,
  };
}

export function collectExecutionDetailsFailedAction(): Action {
  return {
    type: COLLECT_EXECUTION_DETAILS_FAILED,
  };
}
