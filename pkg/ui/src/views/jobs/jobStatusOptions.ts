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

export enum JobStatusVisual {
  BadgeOnly,
  BadgeWithDuration,
  ProgressBarWithDuration,
  BadgeWithMessage,
  BadgeWithErrorMessage,
}

export function jobToVisual(job: Job): JobStatusVisual {
  if (job.type === "CHANGEFEED") {
    return JobStatusVisual.BadgeOnly;
  }
  switch (job.status) {
    case JOB_STATUS_SUCCEEDED:
      return JobStatusVisual.BadgeWithDuration;
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

export const JOB_STATUS_SUCCEEDED = "succeeded";
export const JOB_STATUS_FAILED = "failed";
export const JOB_STATUS_CANCELED = "canceled";
export const JOB_STATUS_PAUSED = "paused";
export const JOB_STATUS_RUNNING = "running";
export const JOB_STATUS_PENDING = "pending";

export const statusOptions = [
  { value: "", label: "All" },
  { value: "succeeded", label: "Succeeded" },
  { value: "failed", label: "Failed" },
  { value: "running", label: "Running" },
  { value: "pending", label: "Pending" },
  { value: "canceled", label: "Canceled" },
  { value: "paused", label: "Paused" },
];

export function jobHasOneOfStatuses(job: Job, ...statuses: string[]) {
  return statuses.indexOf(job.status) !== -1;
}

export const jobStatusToBadgeStatus = (status: string): BadgeStatus => {
  switch (status) {
    case JOB_STATUS_SUCCEEDED:
      return "success";
    case JOB_STATUS_FAILED:
      return "danger";
    case JOB_STATUS_CANCELED:
      return "default";
    case JOB_STATUS_PAUSED:
      return "default";
    case JOB_STATUS_RUNNING:
      return "info";
    case JOB_STATUS_PENDING:
      return "warning";
    default:
      return "info";
  }
};
