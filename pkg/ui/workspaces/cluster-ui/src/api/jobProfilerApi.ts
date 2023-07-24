// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { fetchData } from "./fetchData";

export type ListJobProfilerExecutionDetailsRequest =
  cockroach.server.serverpb.ListJobProfilerExecutionDetailsRequest;
export type ListJobProfilerExecutionDetailsResponse =
  cockroach.server.serverpb.ListJobProfilerExecutionDetailsResponse;

export const getExecutionDetails = (
  req: ListJobProfilerExecutionDetailsRequest,
): Promise<cockroach.server.serverpb.ListJobProfilerExecutionDetailsResponse> => {
  return fetchData(
    cockroach.server.serverpb.ListJobProfilerExecutionDetailsResponse,
    `/_status/list_job_profiler_execution_details/${req.job_id}`,
    null,
    null,
    "30M",
  );
};
