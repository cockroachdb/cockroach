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
  BadgeWithRetrying,
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
    case JOB_STATUS_RUNNING:
      return JobStatusVisual.ProgressBarWithDuration;
    case JOB_STATUS_RETRY_RUNNING:
      return JobStatusVisual.ProgressBarWithDuration;
    case JOB_STATUS_PENDING:
      return JobStatusVisual.BadgeWithMessage;
    case JOB_STATUS_RETRY_REVERTING:
      return JobStatusVisual.BadgeWithRetrying;
    case JOB_STATUS_CANCELED:
    case JOB_STATUS_CANCEL_REQUESTED:
    case JOB_STATUS_PAUSED:
      return job.error == ""
        ? JobStatusVisual.BadgeOnly
        : JobStatusVisual.BadgeWithErrorMessage;
    case JOB_STATUS_PAUSE_REQUESTED:
    case JOB_STATUS_REVERTING:
    default:
      return JobStatusVisual.BadgeOnly;
  }
}

export const JOB_STATUS_SUCCEEDED = "succeeded";
export const JOB_STATUS_FAILED = "failed";
export const JOB_STATUS_CANCELED = "canceled";
export const JOB_STATUS_CANCEL_REQUESTED = "cancel-requested";
export const JOB_STATUS_PAUSED = "paused";
export const JOB_STATUS_PAUSE_REQUESTED = "pause-requested";
export const JOB_STATUS_RUNNING = "running";
export const JOB_STATUS_RETRY_RUNNING = "retry-running";
export const JOB_STATUS_PENDING = "pending";
export const JOB_STATUS_REVERTING = "reverting";
export const JOB_STATUS_REVERT_FAILED = "revert-failed";
export const JOB_STATUS_RETRY_REVERTING = "retry-reverting";

export function isRetrying(status: string): boolean {
  return [JOB_STATUS_RETRY_RUNNING, JOB_STATUS_RETRY_REVERTING].includes(
    status,
  );
}
export function isRunning(status: string): boolean {
  return [JOB_STATUS_RUNNING, JOB_STATUS_RETRY_RUNNING].includes(status);
}

export function shouldShowHighwaterTimestamp(job: Job): boolean {
  return (
    job.highwater_timestamp &&
    [
      JOB_STATUS_RUNNING,
      JOB_STATUS_PAUSED,
      JOB_STATUS_PAUSE_REQUESTED,
    ].includes(job.status)
  );
}

export const statusOptions = [
  { value: "", label: "All" },
  { value: "succeeded", label: "Succeeded" },
  { value: "failed", label: "Failed" },
  { value: "paused", label: "Paused" },
  { value: "canceled", label: "Canceled" },
  { value: "running", label: "Running" },
  { value: "pending", label: "Pending" },
  { value: "reverting", label: "Reverting" },
  { value: "retrying", label: "Retrying" },
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
    case JOB_STATUS_REVERT_FAILED:
      return "danger";
    case JOB_STATUS_RUNNING:
      return "info";
    case JOB_STATUS_PENDING:
      return "warning";
    case JOB_STATUS_CANCELED:
    case JOB_STATUS_CANCEL_REQUESTED:
    case JOB_STATUS_PAUSED:
    case JOB_STATUS_PAUSE_REQUESTED:
    case JOB_STATUS_REVERTING:
    case JOB_STATUS_RETRY_REVERTING:
    default:
      return "default";
  }
};
export const jobStatusToBadgeText = (status: string): string => {
  switch (status) {
    case JOB_STATUS_RETRY_REVERTING:
      return JOB_STATUS_REVERTING;
    case JOB_STATUS_RETRY_RUNNING:
      return JOB_STATUS_RUNNING;
    default:
      return status;
  }
};
