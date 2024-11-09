// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
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
  if (job.type === "REPLICATION STREAM PRODUCER") {
    return JobStatusVisual.BadgeOnly;
  }
  if (
    job.type === "REPLICATION STREAM INGESTION" ||
    job.type === "LOGICAL REPLICATION"
  ) {
    return jobToVisualForReplicationIngestion(job);
  }
  switch (job.status) {
    case JOB_STATUS_SUCCEEDED:
      return JobStatusVisual.BadgeWithDuration;
    case JOB_STATUS_FAILED:
      return JobStatusVisual.BadgeWithErrorMessage;
    case JOB_STATUS_RUNNING:
      return JobStatusVisual.ProgressBarWithDuration;
    case JOB_STATUS_PENDING:
      return JobStatusVisual.BadgeWithMessage;
    case JOB_STATUS_CANCELED:
    case JOB_STATUS_CANCEL_REQUESTED:
    case JOB_STATUS_PAUSED:
      return job.error === ""
        ? JobStatusVisual.BadgeOnly
        : JobStatusVisual.BadgeWithErrorMessage;
    case JOB_STATUS_PAUSE_REQUESTED:
    case JOB_STATUS_REVERTING:
    default:
      return JobStatusVisual.BadgeOnly;
  }
}

function jobToVisualForReplicationIngestion(job: Job): JobStatusVisual {
  if (job.fraction_completed > 0 && job.status === JOB_STATUS_RUNNING) {
    return JobStatusVisual.ProgressBarWithDuration;
  }
  return JobStatusVisual.BadgeWithMessage;
}

export const JOB_STATUS_SUCCEEDED = "succeeded";
export const JOB_STATUS_FAILED = "failed";
export const JOB_STATUS_CANCELED = "canceled";
export const JOB_STATUS_CANCEL_REQUESTED = "cancel-requested";
export const JOB_STATUS_PAUSED = "paused";
export const JOB_STATUS_PAUSE_REQUESTED = "paused-requested";
export const JOB_STATUS_RUNNING = "running";
export const JOB_STATUS_PENDING = "pending";
export const JOB_STATUS_REVERTING = "reverting";
export const JOB_STATUS_REVERT_FAILED = "revert-failed";

export function isRunning(status: string): boolean {
  return [JOB_STATUS_RUNNING, JOB_STATUS_REVERTING].some(s =>
    status.includes(s),
  );
}
export function isTerminalState(status: string): boolean {
  return [JOB_STATUS_SUCCEEDED, JOB_STATUS_FAILED].includes(status);
}

export const statusOptions = [
  { value: "", name: "All" },
  { value: JOB_STATUS_SUCCEEDED, name: "Succeeded" },
  { value: JOB_STATUS_FAILED, name: "Failed" },
  { value: JOB_STATUS_PAUSED, name: "Paused" },
  { value: JOB_STATUS_PAUSE_REQUESTED, name: "Pause Requested" },
  { value: JOB_STATUS_CANCELED, name: "Canceled" },
  { value: JOB_STATUS_CANCEL_REQUESTED, name: "Cancel Requested" },
  { value: JOB_STATUS_RUNNING, name: "Running" },
  { value: JOB_STATUS_PENDING, name: "Pending" },
  { value: JOB_STATUS_REVERTING, name: "Reverting" },
  { value: JOB_STATUS_REVERT_FAILED, name: "Revert Failed" },
];

const ALL_JOB_STATUSES = new Set(statusOptions.map(option => option.value));

/**
 * @param jobStatus job status - any string
 * @returns Returns true if the job status string is a valid status.
 */
export function isValidJobStatus(jobStatus: string): boolean {
  return ALL_JOB_STATUSES.has(jobStatus);
}

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
    default:
      return "default";
  }
};

const jobTypeKeys = Object.keys(JobType);

export const typeOptions = [
  {
    value: JobType.UNSPECIFIED.toString(),
    name: "All",
    key: jobTypeKeys[JobType.UNSPECIFIED],
  },
  {
    value: JobType.BACKUP.toString(),
    name: "Backups",
    key: jobTypeKeys[JobType.BACKUP],
  },
  {
    value: JobType.RESTORE.toString(),
    name: "Restores",
    key: jobTypeKeys[JobType.RESTORE],
  },
  {
    value: JobType.IMPORT.toString(),
    name: "Imports",
    key: jobTypeKeys[JobType.IMPORT],
  },
  {
    value: JobType.SCHEMA_CHANGE.toString(),
    name: "Schema Changes",
    key: jobTypeKeys[JobType.SCHEMA_CHANGE],
  },
  {
    value: JobType.CHANGEFEED.toString(),
    name: "Changefeed",
    key: jobTypeKeys[JobType.CHANGEFEED],
  },
  {
    value: JobType.CREATE_STATS.toString(),
    name: "Statistics Creation",
    key: jobTypeKeys[JobType.CREATE_STATS],
  },
  {
    value: JobType.AUTO_CREATE_STATS.toString(),
    name: "Auto-Statistics Creation",
    key: jobTypeKeys[JobType.AUTO_CREATE_STATS],
  },
  {
    value: JobType.SCHEMA_CHANGE_GC.toString(),
    name: "Schema Change GC",
    key: jobTypeKeys[JobType.SCHEMA_CHANGE_GC],
  },
  {
    value: JobType.TYPEDESC_SCHEMA_CHANGE.toString(),
    name: "Type Descriptor Schema Changes",
    key: jobTypeKeys[JobType.TYPEDESC_SCHEMA_CHANGE],
  },
  {
    value: JobType.NEW_SCHEMA_CHANGE.toString(),
    name: "New Schema Changes",
    key: jobTypeKeys[JobType.NEW_SCHEMA_CHANGE],
  },
  {
    value: JobType.MIGRATION.toString(),
    name: "Migrations",
    key: jobTypeKeys[JobType.MIGRATION],
  },
  {
    value: JobType.REPLICATION_STREAM_INGESTION.toString(),
    name: "Physical Replication Ingestion",
    key: jobTypeKeys[JobType.REPLICATION_STREAM_INGESTION],
  },
  {
    value: JobType.REPLICATION_STREAM_PRODUCER.toString(),
    name: "Replication Producer",
    key: jobTypeKeys[JobType.REPLICATION_STREAM_PRODUCER],
  },
  {
    value: JobType.LOGICAL_REPLICATION.toString(),
    name: "Logical Replication Ingestion",
    key: jobTypeKeys[JobType.LOGICAL_REPLICATION],
  },
  {
    value: JobType.AUTO_SPAN_CONFIG_RECONCILIATION.toString(),
    name: "Span Config Reconciliation",
    key: jobTypeKeys[JobType.AUTO_SPAN_CONFIG_RECONCILIATION],
  },
  {
    value: JobType.AUTO_SQL_STATS_COMPACTION.toString(),
    name: "SQL Stats Compactions",
    key: jobTypeKeys[JobType.AUTO_SQL_STATS_COMPACTION],
  },
  {
    value: JobType.ROW_LEVEL_TTL.toString(),
    name: "Time-to-live Deletions",
    key: jobTypeKeys[JobType.ROW_LEVEL_TTL],
  },
];

export function isValidJobType(jobType: number): boolean {
  return jobType >= 0 && jobType < jobTypeKeys.length;
}

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
