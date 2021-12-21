// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { JobTableProps } from "./jobTable";
import moment from "moment";
import * as protos from "@cockroachlabs/crdb-protobuf-client";
import { cockroach } from "src/js/protos";
import JobsResponse = cockroach.server.serverpb.JobsResponse;
import Job = cockroach.server.serverpb.IJobResponse;
import Long from "long";

const defaultJobProperties = {
  username: "root",
  descriptor_ids: [] as number[],
  created: new protos.google.protobuf.Timestamp({
    seconds: new Long(1634648118),
    nanos: 200459000,
  }),
  started: new protos.google.protobuf.Timestamp({
    seconds: new Long(1634648118),
    nanos: 215527000,
  }),
  finished: new protos.google.protobuf.Timestamp({
    seconds: new Long(1634648118),
    nanos: 311522000,
  }),
  modified: new protos.google.protobuf.Timestamp({
    seconds: new Long(1634648118),
    nanos: 310899000,
  }),
  fraction_completed: 1,
  last_run: new protos.google.protobuf.Timestamp({
    seconds: new Long(1634648118),
    nanos: 215527000,
  }),
  next_run: new protos.google.protobuf.Timestamp({
    seconds: new Long(1634648118),
    nanos: 215527100,
  }),
  num_runs: new Long(1),
};

export const succeededJobFixture = {
  ...defaultJobProperties,
  id: new Long(8136728577, 70289336),
  type: "AUTO SQL STATS COMPACTION",
  description: "automatic SQL Stats compaction",
  username: "node",
  status: "succeeded",
};

const failedJobFixture = {
  ...defaultJobProperties,
  id: new Long(7003330561, 70312826),
  type: "SCHEMA CHANGE",
  description:
    "ALTER TABLE movr.public.user_promo_codes ADD FOREIGN KEY (city, user_id) REFERENCES movr.public.users (city, id)",
  status: "failed",
  error: "mock failure message",
};

const canceledJobFixture = {
  ...defaultJobProperties,
  id: new Long(7002707969, 70312826),
  type: "UNSPECIFIED",
  description: "Unspecified",
  status: "canceled",
};

const pausedJobFixture = {
  ...defaultJobProperties,
  id: new Long(6091954177, 70312826),
  type: "BACKUP",
  description:
    "BACKUP DATABASE bank TO 'gs://acme-co-backup/database-bank-2017-03-29-nightly' AS OF SYSTEM TIME '-10s' INCREMENTAL FROM 'gs://acme-co-backup/database-bank-2017-03-27-weekly', 'gs://acme-co-backup/database-bank-2017-03-28-nightly' WITH revision_history\n",
  status: "paused",
};

const runningJobFixture = {
  ...defaultJobProperties,
  id: new Long(3390625793, 70312826),
  type: "AUTO SPAN CONFIG RECONCILIATION",
  description: "GC for DROP TABLE havent_started_running",
  status: "running",
  fraction_completed: 0,
};

const runningWithMessageJobFixture = {
  ...defaultJobProperties,
  id: new Long(3390625793, 70312826),
  type: "AUTO SPAN CONFIG RECONCILIATION",
  description: "GC for DROP TABLE no_duration_has_running_status",
  status: "running",
  fraction_completed: 0,
  running_status: "Waiting For GC TTL",
};

const runningWithRemainingJobFixture = {
  ...defaultJobProperties,
  id: new Long(6093756417, 70312826),
  type: "RESTORE",
  description: "RESTORE data.* FROM $1 WITH OPTIONS (into_db='data2')",
  status: "running",
  fraction_completed: 0.38,
};

const runningWithMessageRemainingJobFixture = {
  ...defaultJobProperties,
  id: new Long(3390625793, 70312826),
  type: "AUTO SPAN CONFIG RECONCILIATION",
  description: "automatic Span Config reconciliation",
  status: "running",
  fraction_completed: 0.66,
  running_status: "performing garbage collection on index 2",
};

export const retryRunningJobFixture = {
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

const retryRunningWithMessageJobFixture = {
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

const retryRunningWithRemainingJobFixture = {
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

const retryRunningWithMessageRemainingJobFixture = {
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

const pendingJobFixture = {
  ...defaultJobProperties,
  id: new Long(5247850497, 70312826),
  type: "IMPORT",
  description:
    "IMPORT PGDUMP 'userfile://defaultdb.public.userfiles_root/db.sql' WITH max_row_size='524288'",
  status: "pending",
};

const revertingJobFixture = {
  ...defaultJobProperties,
  id: new Long(5246539777, 70312826),
  type: "CHANGEFEED",
  description: "CREATE CHANGEFEED FOR foo WITH updated, resolved, diff",
  status: "reverting",
};

const retryRevertingJobFixture = {
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

const highwaterJobFixture = {
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

const cancelRequestedJobFixture = {
  ...defaultJobProperties,
  id: new Long(4337653761, 70312826),
  type: "CREATE STATS",
  description: "SELECT job_id, job_type FROM [SHOW JOB 1]",
  status: "cancel-requested",
};

const pauseRequestedJobFixture = {
  ...defaultJobProperties,
  id: new Long(4338669569, 70312826),
  type: "AUTO CREATE STATS",
  description: "SELECT job_id, job_type FROM [SHOW AUTOMATIC JOBS];",
  status: "pause-requested",
};

const revertFailedJobFixture = {
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

const getJobTableProps = (jobs: Array<Job>): JobTableProps => ({
  sort: {
    columnTitle: "creationTime",
    ascending: false,
  },
  isUsedFilter: false,
  setSort: (() => {}) as any,
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

export const withData: JobTableProps = getJobTableProps(allJobsFixture);
export const empty: JobTableProps = getJobTableProps([]);
