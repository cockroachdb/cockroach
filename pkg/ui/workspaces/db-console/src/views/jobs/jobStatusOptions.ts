// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { cockroach } from "src/js/protos";
import Job = cockroach.server.serverpb.IJobResponse;
import { BadgeStatus } from "src/components";
import moment from "moment";
import { TimestampToMoment } from "src/util/convert";
import Long from "long";

export enum JobStatusVisual {
  BadgeOnly,
  BadgeWithDuration,
  BadgeWithNextExecutionTime,
  ProgressBarWithDuration,
  BadgeWithMessage,
  BadgeWithErrorMessage,
}

export function jobToVisual(job: Job): JobStatusVisual {
  if (job.type === "CHANGEFEED") {
    return JobStatusVisual.BadgeOnly;
  }
  if (jobToDisplayStatus(job) == JOB_DISPLAY_STATUS_RETRYING) {
    return JobStatusVisual.BadgeWithNextExecutionTime;
  }
  switch (job.status) {
    case JOB_STATUS_SUCCEEDED:
    case JOB_STATUS_FAILED:
      return JobStatusVisual.BadgeWithErrorMessage;
    case JOB_STATUS_CANCELED:
      return JobStatusVisual.BadgeOnly;
    case JOB_STATUS_PAUSED:
      return JobStatusVisual.BadgeOnly;
    case JOB_STATUS_RUNNING:
      return JobStatusVisual.ProgressBarWithDuration;
    case JOB_STATUS_PENDING:
      return JobStatusVisual.BadgeWithMessage;
    default:
      return JobStatusVisual.BadgeOnly;
  }
}

// export enum JobStatus {
//   BadgeOnly,
//   BadgeWithDuration,
//   BadgeWithNextExecutionTime,
//   ProgressBarWithDuration,
//   BadgeWithMessage,
//   BadgeWithErrorMessage,
// }
//
// export type JobDisplayStatus = JobStatus | enum {
//   asdf
// }

export const JOB_STATUS_SUCCEEDED = "succeeded";
export const JOB_STATUS_FAILED = "failed";
export const JOB_STATUS_CANCELED = "canceled";
export const JOB_STATUS_PAUSED = "paused";
export const JOB_STATUS_RUNNING = "running";
export const JOB_STATUS_PENDING = "pending";
export const JOB_STATUS_REVERTING = "reverting";
export const JOB_DISPLAY_STATUS_RETRYING = "retrying";

type JobStatus =
  | JOB_STATUS_SUCCEEDED
  | JOB_STATUS_FAILED
  | JOB_STATUS_CANCELED
  | JOB_STATUS_PAUSED
  | JOB_STATUS_RUNNING
  | JOB_STATUS_PENDING
  | JOB_STATUS_REVERTING;

type JobDisplayStatus = JobStatus | JOB_STATUS_RETRYING;

export const jobToDisplayStatus = (job: Job): JobDisplayStatus => {
  const now = moment();
  if (
    [JOB_STATUS_RUNNING, JOB_STATUS_REVERTING].includes(job.status) &&
    TimestampToMoment(job.next_run).isAfter(now) &&
    job.num_runs > new Long(0)
  ) {
    return JOB_DISPLAY_STATUS_RETRYING;
  }
  return job.status;
};

export const statusOptions = [
  { value: "", label: "All" },
  { value: "succeeded", label: "Succeeded" },
  { value: "failed", label: "Failed" },
  { value: "canceled", label: "Canceled" },
  { value: "paused", label: "Paused" },
  { value: "running", label: "Running" },
  { value: "pending", label: "Pending" },
];

export function jobHasOneOfStatuses(job: Job, ...statuses: string[]) {
  return statuses.indexOf(job.status) !== -1;
}

export const jobDisplayStatusToBadgeStatus = (status: string): BadgeStatus => {
  switch (status) {
    case JOB_STATUS_SUCCEEDED:
      return "success";
    case JOB_STATUS_FAILED:
      return "danger";
    case JOB_STATUS_CANCELED:
      return "default";
    case JOB_STATUS_PAUSED:
      return "default";
    case JOB_STATUS_REVERTING:
      return "default";
    case JOB_STATUS_RUNNING:
      return "info";
    case JOB_STATUS_PENDING:
      return "warning";
    case JOB_DISPLAY_STATUS_RETRYING:
      return "warning";
    default:
      return "info";
  }
};
