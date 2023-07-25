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
import { SqlExecutionRequest, executeInternalSql } from "./sqlApi";

export type ListJobProfilerExecutionDetailsRequest =
  cockroach.server.serverpb.ListJobProfilerExecutionDetailsRequest;
export type ListJobProfilerExecutionDetailsResponse =
  cockroach.server.serverpb.ListJobProfilerExecutionDetailsResponse;

export type GetJobProfilerExecutionDetailRequest =
  cockroach.server.serverpb.GetJobProfilerExecutionDetailRequest;
export type GetJobProfilerExecutionDetailResponse =
  cockroach.server.serverpb.GetJobProfilerExecutionDetailResponse;

export const listExecutionDetailFiles = (
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

export const getExecutionDetailFile = (
  req: GetJobProfilerExecutionDetailRequest,
): Promise<cockroach.server.serverpb.GetJobProfilerExecutionDetailResponse> => {
  return fetchData(
    cockroach.server.serverpb.GetJobProfilerExecutionDetailResponse,
    `/_status/job_profiler_execution_details/${req.job_id}/${req.filename}`,
    null,
    null,
    "30M",
  );
};

export type CollectExecutionDetailsRequest = {
  job_id: Long;
};

export type CollectExecutionDetailsResponse = {
  req_resp: boolean;
};

export function collectExecutionDetails({
  job_id,
}: CollectExecutionDetailsRequest): Promise<CollectExecutionDetailsResponse> {
  const args: any = [job_id.toString()];

  const collectExecutionDetails = {
    sql: `SELECT crdb_internal.request_job_execution_details($1::INT) as req_resp`,
    arguments: args,
  };

  const req: SqlExecutionRequest = {
    execute: true,
    statements: [collectExecutionDetails],
  };

  return executeInternalSql<CollectExecutionDetailsResponse>(req).then(res => {
    // If request succeeded but query failed, throw error (caught by saga/cacheDataReducer).
    if (res.error) {
      throw res.error;
    }

    if (
      res.execution?.txn_results[0]?.rows?.length === 0 ||
      res.execution?.txn_results[0]?.rows[0]["req_resp"] === false
    ) {
      throw new Error("Failed to collect execution details");
    }

    return res.execution.txn_results[0].rows[0];
  });
}
