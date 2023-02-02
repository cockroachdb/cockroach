// Copyright 2022 The Cockroach Authors.
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
import { propsToQueryString } from "../util";

const JOBS_PATH = "/_admin/v1/jobs";

export type JobsRequest = cockroach.server.serverpb.JobsRequest;
export type JobsResponse = cockroach.server.serverpb.JobsResponse;

export type JobRequest = cockroach.server.serverpb.JobRequest;
export type JobResponse = cockroach.server.serverpb.JobResponse;
export type JobResponseWithKey = {
  jobResponse: JobResponse;
  key: string;
};
export type ErrorWithKey = {
  err: Error;
  key: string;
};

export const getJobs = (
  req: JobsRequest,
): Promise<cockroach.server.serverpb.JobsResponse> => {
  let jobsRequestPath = `${JOBS_PATH}`;
  const queryStr = propsToQueryString({
    status: req.status,
    type: req.type.toString(),
    limit: req.limit,
  });
  if (queryStr) {
    jobsRequestPath = jobsRequestPath.concat(`?${queryStr}`);
  }
  return fetchData(
    cockroach.server.serverpb.JobsResponse,
    jobsRequestPath,
    null,
    null,
    "30M",
  );
};

export const getJob = (
  req: JobRequest,
): Promise<cockroach.server.serverpb.JobResponse> => {
  return fetchData(
    cockroach.server.serverpb.JobResponse,
    `${JOBS_PATH}/${req.job_id}`,
    null,
    null,
    "30M",
  );
};
