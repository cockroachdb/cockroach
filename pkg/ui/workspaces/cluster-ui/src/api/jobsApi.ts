// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach, google } from "@cockroachlabs/crdb-protobuf-client";
import long from "long";
import { SWRConfiguration } from "swr";

import { propsToQueryString, useSwrWithClusterId } from "../util";

import { fetchDataJSON } from "./fetchData";

const JOBS_PATH = "api/v2/dbconsole/jobs";

export type JobsRequest = cockroach.server.serverpb.JobsRequest;
export type JobsResponse = cockroach.server.serverpb.JobsResponse;
type IJobsResponse = cockroach.server.serverpb.IJobsResponse;

export type JobRequest = cockroach.server.serverpb.JobRequest;
export type JobResponse = cockroach.server.serverpb.JobResponse;
type IJobResponse = cockroach.server.serverpb.IJobResponse;

export type JobResponseWithKey = {
  jobResponse: JobResponse;
  key: string;
};
export type ErrorWithKey = {
  err: Error;
  key: string;
};

// BFF response types (private, match the Go BFF handler structs).
// TODO(jasonlmfong): these are temporary shim types for the BFF migration.
// Once downstream consumers are updated to use BFF types directly,
// remove the Bff* interfaces and the bffJobToJobResponse conversion.
interface BffJobExecutionFailure {
  status: string;
  start: string;
  end: string;
  error: string;
}

interface BffJobMessage {
  kind: string;
  timestamp: string;
  message: string;
}

interface BffJobInfo {
  id: string;
  type: string;
  description: string;
  statement: string;
  username: string;
  status: string;
  running_status: string;
  created: string;
  started: string;
  finished: string;
  modified: string;
  fraction_completed: number;
  error: string;
  highwater_timestamp: string;
  highwater_decimal: string;
  last_run: string;
  next_run: string;
  num_runs: number;
  coordinator_id: number;
  execution_failures: BffJobExecutionFailure[];
  messages: BffJobMessage[];
}

interface BffJobsListResponse {
  jobs: BffJobInfo[];
  earliest_retained_time: string;
}

// isoToTimestamp converts an ISO 8601 string to a protobuf ITimestamp-compatible
// object with Long seconds and numeric nanos. Consumers call
// timestamp.seconds.toNumber() and access timestamp.nanos, so both fields must
// be present.
function isoToTimestamp(iso: string): google.protobuf.ITimestamp | null {
  if (!iso) return null;
  const ms = new Date(iso).getTime();
  if (isNaN(ms)) return null;
  return {
    seconds: long.fromNumber(Math.floor(ms / 1000)),
    nanos: (ms % 1000) * 1e6,
  };
}

// bffJobToJobResponse converts a BFF JSON job into a protobuf-compatible
// JobResponse object. Numeric IDs become Long values and ISO timestamps become
// {seconds: Long, nanos} objects so that existing consumer code (which calls
// .toNumber(), .isZero(), TimestampToMoment(), etc.) continues to work.
function bffJobToJobResponse(bff: BffJobInfo): JobResponse {
  // satisfies IJobResponse validates field names and types against the protobuf
  // interface. The as-cast is only needed because we construct a plain object
  // rather than a class instance; consumers expect the class type.
  return {
    id: long.fromString(bff.id),
    type: bff.type,
    description: bff.description,
    statement: bff.statement,
    username: bff.username,
    descriptor_ids: [], // not provided by BFF; unused by UI consumers
    status: bff.status,
    running_status: bff.running_status,
    created: isoToTimestamp(bff.created),
    started: isoToTimestamp(bff.started),
    finished: isoToTimestamp(bff.finished),
    modified: isoToTimestamp(bff.modified),
    fraction_completed: bff.fraction_completed,
    error: bff.error,
    highwater_timestamp: isoToTimestamp(bff.highwater_timestamp),
    highwater_decimal: bff.highwater_decimal,
    last_run: isoToTimestamp(bff.last_run),
    next_run: isoToTimestamp(bff.next_run),
    num_runs: long.fromNumber(bff.num_runs),
    coordinator_id: long.fromNumber(bff.coordinator_id),
    execution_failures: (bff.execution_failures || []).map(f => ({
      status: f.status,
      start: isoToTimestamp(f.start),
      end: isoToTimestamp(f.end),
      error: f.error,
    })),
    messages: (bff.messages || []).map(m => ({
      kind: m.kind,
      timestamp: isoToTimestamp(m.timestamp),
      message: m.message,
    })),
  } satisfies IJobResponse as JobResponse;
}

export const getJobs = (req: JobsRequest): Promise<JobsResponse> => {
  const queryStr = propsToQueryString({
    status: req.status,
    type: req.type || undefined,
    limit: req.limit,
  });
  const path = queryStr ? `${JOBS_PATH}/?${queryStr}` : `${JOBS_PATH}/`;
  return fetchDataJSON<BffJobsListResponse, void>(path).then(resp => {
    return {
      jobs: (resp.jobs || []).map(bffJobToJobResponse),
      earliest_retained_time: isoToTimestamp(resp.earliest_retained_time),
    } satisfies IJobsResponse as JobsResponse;
  });
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

export const getJob = (req: JobRequest): Promise<JobResponse> => {
  return fetchDataJSON<BffJobInfo, void>(`${JOBS_PATH}/${req.job_id}/`).then(
    bffJobToJobResponse,
  );
};
