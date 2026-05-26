// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import long from "long";
import { SWRConfiguration } from "swr";

import { propsToQueryString, useSwrWithClusterId } from "../util";

import { fetchData } from "./fetchData";

type JobType = cockroach.sql.jobs.jobspb.Type;

const JOBS_PATH = "_admin/v1/jobs";

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

export function useJobDetails(jobId: long, opts: SWRConfiguration = {}) {
  return useSwrWithClusterId<JobResponse>(
    { name: "jobDetailsById", jobId },
    () => getJob({ job_id: jobId }),
    {
      revalidateOnFocus: false,
      revalidateOnReconnect: false,
      ...opts,
    },
  );
}

export function useJobs(
  status: string,
  type: JobType,
  limit: number,
  opts: SWRConfiguration = {},
) {
  return useSwrWithClusterId<JobsResponse>(
    { name: "jobs", status, type: String(type), limit: String(limit) },
    () =>
      getJobs(
        new cockroach.server.serverpb.JobsRequest({ status, type, limit }),
      ),
    {
      revalidateOnFocus: false,
      revalidateOnReconnect: false,
      ...opts,
    },
  );
}

export const getJob = (req: JobRequest): Promise<JobResponse> => {
  return fetchData(
    cockroach.server.serverpb.JobResponse,
    `${JOBS_PATH}/${req.job_id}`,
    null,
    null,
    "30M",
  );
};
