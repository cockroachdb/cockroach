// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { JobsTableProps } from "src/views/jobs/index";
import moment from "moment";
import * as protos from "@cockroachlabs/crdb-protobuf-client";
import { cockroach } from "src/js/protos";
import JobsResponse = cockroach.server.serverpb.JobsResponse;
import Job = cockroach.server.serverpb.IJobResponse;
import Long from "long";
import { createMemoryHistory } from "history";
import { jobsTimeoutErrorMessage } from "src/util/api";
import { defaultJobProperties } from "src/views/jobs/jobsShared.fixture";

export const succeededJobFixture: Job = {
  ...defaultJobProperties,
  id: new Long(8136728577, 70289336),
  type: "AUTO SQL STATS COMPACTION",
  description: "automatic SQL Stats compaction",
  username: "node",
  status: "succeeded",
};

const failedJobFixture: Job = {
  ...defaultJobProperties,
  id: new Long(7003330561, 70312826),
  type: "SCHEMA CHANGE",
  description:
    "ALTER TABLE movr.public.user_promo_codes ADD FOREIGN KEY (city, user_id) REFERENCES movr.public.users (city, id)",
  status: "failed",
  error: "mock failure message",
};

const canceledJobFixture: Job = {
  ...defaultJobProperties,
  id: new Long(7002707969, 70312826),
  type: "UNSPECIFIED",
  description: "Unspecified",
  status: "canceled",
};

const pausedJobFixture: Job = {
  ...defaultJobProperties,
  id: new Long(6091954177, 70312826),
  type: "BACKUP",
  description:
    "BACKUP DATABASE bank TO 'gs://acme-co-backup/database-bank-2017-03-29-nightly' AS OF SYSTEM TIME '-10s' INCREMENTAL FROM 'gs://acme-co-backup/database-bank-2017-03-27-weekly', 'gs://acme-co-backup/database-bank-2017-03-28-nightly' WITH revision_history\n",
  status: "paused",
};

const runningJobFixture: Job = {
  ...defaultJobProperties,
  id: new Long(3390625793, 70312826),
  type: "AUTO SPAN CONFIG RECONCILIATION",
  description: "GC for DROP TABLE havent_started_running",
  status: "running",
  fraction_completed: 0,
};

const runningWithMessageJobFixture: Job = {
  ...defaultJobProperties,
  id: new Long(3390625793, 70312826),
  type: "AUTO SPAN CONFIG RECONCILIATION",
  description: "GC for DROP TABLE no_duration_has_running_status",
  status: "running",
  fraction_completed: 0,
  running_status: "Waiting For GC TTL",
};

const runningWithRemainingJobFixture: Job = {
  ...defaultJobProperties,
  id: new Long(6093756417, 70312826),
  type: "RESTORE",
  description: "RESTORE data.* FROM $1 WITH OPTIONS (into_db='data2')",
  status: "running",
  fraction_completed: 0.38,
};

const runningWithMessageRemainingJobFixture: Job = {
  ...defaultJobProperties,
  id: new Long(3390625793, 70312826),
  type: "AUTO SPAN CONFIG RECONCILIATION",
  description: "automatic Span Config reconciliation",
  status: "running",
  fraction_completed: 0.66,
  running_status: "performing garbage collection on index 2",
};

export const retryRunningJobFixture: Job = {
  ...defaultJobProperties,
  id: new Long(3390625793, 70312826),
  type: "STREAM INGESTION",
  description: "GC for DROP TABLE havent_started_running_2",
  status: "retry-running",
  fraction_completed: 0,
  next_run: new protos.google.protobuf.Timestamp({
    seconds: new Long(3034648417), // some long time in the future, because it needs to be in the future to show as retrying
    nanos: 116912000,
  }),
  num_runs: new Long(3),
};

const retryRunningWithMessageJobFixture: Job = {
  ...defaultJobProperties,
  id: new Long(3390625793, 70312826),
  type: "AUTO SPAN CONFIG RECONCILIATION",
  description: "GC for DROP TABLE no_duration_has_running_status",
  status: "retry-running",
  fraction_completed: 0,
  running_status: "Waiting For GC TTL",
  next_run: new protos.google.protobuf.Timestamp({
    seconds: new Long(3034648417), // some long time in the future, because it needs to be in the future to show as retrying
    nanos: 116912000,
  }),
  num_runs: new Long(2),
};

const retryRunningWithRemainingJobFixture: Job = {
  ...defaultJobProperties,
  id: new Long(3390625793, 70312826),
  type: "STREAM INGESTION",
  description:
    "RESTORE DATABASE backup_database_name FROM 'your_backup_location';",
  status: "retry-running",
  fraction_completed: 0.11,
  next_run: new protos.google.protobuf.Timestamp({
    seconds: new Long(3034648417), // some long time in the future, because it needs to be in the future to show as retrying
    nanos: 116912000,
  }),
  num_runs: new Long(3),
};

const retryRunningWithMessageRemainingJobFixture: Job = {
  ...defaultJobProperties,
  id: new Long(3390625793, 70312826),
  type: "MIGRATION",
  description:
    "IMPORT MYSQLDUMP 'https://s3-us-west-1.amazonaws.com/cockroachdb-movr/datasets/employees-db/mysqldump/employees-full.sql.gz';",
  status: "retry-running",
  fraction_completed: 0.82,
  running_status: "performing garbage collection on table crl.roachers",
  next_run: new protos.google.protobuf.Timestamp({
    seconds: new Long(3034648417), // some long time in the future, because it needs to be in the future to show as retrying
    nanos: 116912000,
  }),
  num_runs: new Long(2),
};

const pendingJobFixture: Job = {
  ...defaultJobProperties,
  id: new Long(5247850497, 70312826),
  type: "IMPORT",
  description:
    "IMPORT PGDUMP 'userfile://defaultdb.public.userfiles_root/db.sql' WITH max_row_size='524288'",
  status: "pending",
};

const revertingJobFixture: Job = {
  ...defaultJobProperties,
  id: new Long(5246539777, 70312826),
  type: "CHANGEFEED",
  description: "CREATE CHANGEFEED FOR foo WITH updated, resolved, diff",
  status: "reverting",
};

const retryRevertingJobFixture: Job = {
  ...defaultJobProperties,
  id: new Long(3390625793, 70312826),
  type: "NEW SCHEMA CHANGE",
  description: "ALTER TABLE db.t ADD COLUMN b INT DEFAULT 1",
  status: "retry-reverting",
  next_run: new protos.google.protobuf.Timestamp({
    seconds: new Long(3034648417), // some long time in the future, because it needs to be in the future to show as retrying
    nanos: 116912000,
  }),
  num_runs: new Long(2),
};

const highwaterJobFixture: Job = {
  ...defaultJobProperties,
  id: new Long(3390625793, 70312826),
  type: "TYPEDESC SCHEMA CHANGE",
  description: "ALTER TYPE status ADD VALUE 'pending';",
  status: "running",
  highwater_timestamp: new protos.google.protobuf.Timestamp({
    seconds: new Long(1634648117),
    nanos: 98294000,
  }),
  highwater_decimal: "test highwater decimal",
};

const cancelRequestedJobFixture: Job = {
  ...defaultJobProperties,
  id: new Long(4337653761, 70312826),
  type: "CREATE STATS",
  description: "SELECT job_id, job_type FROM [SHOW JOB 1]",
  status: "cancel-requested",
};

const pauseRequestedJobFixture: Job = {
  ...defaultJobProperties,
  id: new Long(4338669569, 70312826),
  type: "AUTO CREATE STATS",
  description: "SELECT job_id, job_type FROM [SHOW AUTOMATIC JOBS];",
  status: "pause-requested",
};

const revertFailedJobFixture: Job = {
  ...defaultJobProperties,
  id: new Long(3391379457, 70312826),
  type: "SCHEMA CHANGE GC",
  description: "GC for DROP DATABASE t CASCADE",
  status: "revert-failed",
};

export const allJobsFixture = [
  succeededJobFixture,
  failedJobFixture,
  canceledJobFixture,
  pausedJobFixture,
  runningJobFixture,
  runningWithMessageJobFixture,
  runningWithRemainingJobFixture,
  runningWithMessageRemainingJobFixture,
  retryRunningJobFixture,
  retryRunningWithMessageJobFixture,
  retryRunningWithRemainingJobFixture,
  retryRunningWithMessageRemainingJobFixture,
  pendingJobFixture,
  revertingJobFixture,
  retryRevertingJobFixture,
  highwaterJobFixture,
  cancelRequestedJobFixture,
  pauseRequestedJobFixture,
  revertFailedJobFixture,
];

const history = createMemoryHistory({ initialEntries: ["/statements"] });

const staticJobProps: Pick<
  JobsTableProps,
  | "history"
  | "location"
  | "match"
  | "sort"
  | "status"
  | "show"
  | "type"
  | "setSort"
  | "setStatus"
  | "setShow"
  | "setType"
  | "refreshJobs"
> = {
  history,
  location: {
    pathname: "/jobs",
    search: "",
    hash: "",
    state: null,
  },
  match: {
    path: "/jobs",
    url: "/jobs",
    isExact: true,
    params: "{}",
  },
  sort: {
    columnTitle: "creationTime",
    ascending: false,
  },
  status: "",
  show: "50",
  type: 0,
  setSort: () => {},
  setStatus: () => {},
  setShow: () => {},
  setType: () => {},
  refreshJobs: () => null,
};

const getJobsTableProps = (jobs: Array<Job>): JobsTableProps => ({
  ...staticJobProps,
  jobs: {
    inFlight: false,
    valid: false,
    requestedAt: moment(
      "Mon Oct 18 2021 14:01:45 GMT-0400 (Eastern Daylight Time)",
    ),
    setAt: moment("Mon Oct 18 2021 14:01:50 GMT-0400 (Eastern Daylight Time)"),
    lastError: null,
    data: JobsResponse.create({
      jobs: jobs,
    }),
  },
});

export const withData: JobsTableProps = getJobsTableProps(allJobsFixture);
export const empty: JobsTableProps = getJobsTableProps([]);
export const loading: JobsTableProps = {
  ...staticJobProps,
  jobs: {
    inFlight: true,
    valid: false,
    requestedAt: moment(
      "Mon Oct 18 2021 14:01:45 GMT-0400 (Eastern Daylight Time)",
    ),
  },
};

export const error: JobsTableProps = {
  ...staticJobProps,
  jobs: {
    inFlight: false,
    valid: false,
    requestedAt: moment(
      "Mon Oct 18 2021 14:01:45 GMT-0400 (Eastern Daylight Time)",
    ),
    lastError: new Error(jobsTimeoutErrorMessage),
  },
};
