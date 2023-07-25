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
