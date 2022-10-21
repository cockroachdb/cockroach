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
import { BadgeStatus } from "src/badge";

const JobType = cockroach.sql.jobs.jobspb.Type;
type Job = cockroach.server.serverpb.IJobResponse;

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
export const JOB_STATUS_PAUSE_REQUESTED = "paused-requested";
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
export function isTerminalState(status: string): boolean {
  return [JOB_STATUS_SUCCEEDED, JOB_STATUS_FAILED].includes(status);
}

export const statusOptions = [
  { value: "", name: "All" },
  { value: "succeeded", name: "Succeeded" },
  { value: "failed", name: "Failed" },
  { value: "paused", name: "Paused" },
  { value: "canceled", name: "Canceled" },
  { value: "running", name: "Running" },
  { value: "pending", name: "Pending" },
  { value: "reverting", name: "Reverting" },
  { value: "retrying", name: "Retrying" },
];

export function jobHasOneOfStatuses(job: Job, ...statuses: string[]): boolean {
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

export const typeOptions = [
  {
    value: JobType.UNSPECIFIED.toString(),
    name: "All",
    key: Object.keys(JobType)[JobType.UNSPECIFIED],
  },
  {
    value: JobType.BACKUP.toString(),
    name: "Backups",
    key: Object.keys(JobType)[JobType.BACKUP],
  },
  {
    value: JobType.RESTORE.toString(),
    name: "Restores",
    key: Object.keys(JobType)[JobType.RESTORE],
  },
  {
    value: JobType.IMPORT.toString(),
    name: "Imports",
    key: Object.keys(JobType)[JobType.IMPORT],
  },
  {
    value: JobType.SCHEMA_CHANGE.toString(),
    name: "Schema Changes",
    key: Object.keys(JobType)[JobType.SCHEMA_CHANGE],
  },
  {
    value: JobType.CHANGEFEED.toString(),
    name: "Changefeed",
    key: Object.keys(JobType)[JobType.CHANGEFEED],
  },
  {
    value: JobType.CREATE_STATS.toString(),
    name: "Statistics Creation",
    key: Object.keys(JobType)[JobType.CREATE_STATS],
  },
  {
    value: JobType.AUTO_CREATE_STATS.toString(),
    name: "Auto-Statistics Creation",
    key: Object.keys(JobType)[JobType.AUTO_CREATE_STATS],
  },
  {
    value: JobType.SCHEMA_CHANGE_GC.toString(),
    name: "Schema Change GC",
    key: Object.keys(JobType)[JobType.SCHEMA_CHANGE_GC],
  },
  {
    value: JobType.TYPEDESC_SCHEMA_CHANGE.toString(),
    name: "Type Descriptor Schema Changes",
    key: Object.keys(JobType)[JobType.TYPEDESC_SCHEMA_CHANGE],
  },
  {
    value: JobType.STREAM_INGESTION.toString(),
    name: "Stream Ingestion",
    key: Object.keys(JobType)[JobType.STREAM_INGESTION],
  },
  {
    value: JobType.NEW_SCHEMA_CHANGE.toString(),
    name: "New Schema Changes",
    key: Object.keys(JobType)[JobType.NEW_SCHEMA_CHANGE],
  },
  {
    value: JobType.MIGRATION.toString(),
    name: "Migrations",
    key: Object.keys(JobType)[JobType.MIGRATION],
  },
  {
    value: JobType.AUTO_SPAN_CONFIG_RECONCILIATION.toString(),
    name: "Span Config Reconciliation",
    key: Object.keys(JobType)[JobType.AUTO_SPAN_CONFIG_RECONCILIATION],
  },
  {
    value: JobType.AUTO_SQL_STATS_COMPACTION.toString(),
    name: "SQL Stats Compactions",
    key: Object.keys(JobType)[JobType.AUTO_SQL_STATS_COMPACTION],
  },
  {
    value: JobType.STREAM_REPLICATION.toString(),
    name: "Stream Replication",
    key: Object.keys(JobType)[JobType.STREAM_REPLICATION],
  },
  {
    value: JobType.ROW_LEVEL_TTL.toString(),
    name: "Time-to-live Deletions",
    key: Object.keys(JobType)[JobType.ROW_LEVEL_TTL],
  },
];

export const showOptions = [
  { value: "50", name: "Latest 50" },
  { value: "0", name: "All" },
];

export const defaultRequestOptions = {
  limit: 0,
  status: "",
  type: JobType.UNSPECIFIED,
};

export const defaultLocalOptions = {
  show: defaultRequestOptions.limit.toString(),
  status: defaultRequestOptions.status,
  type: Number(defaultRequestOptions.type),
};
