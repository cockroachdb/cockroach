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
import * as protos from "@cockroachlabs/crdb-protobuf-client";
import { createMemoryHistory } from "history";
import Long from "long";
import { SchedulesPageProps } from "./schedulesPage";

import SchedulesResponse = cockroach.server.serverpb.SchedulesResponse;
import Schedule = cockroach.server.serverpb.IScheduleResponse;

const schedulesTimeoutErrorMessage =
  "Unable to retrieve the Schedules table. To reduce the amount of data, try filtering the table.";

const defaultScheduleProperties = {
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

export const succeededScheduleFixture = {
  ...defaultScheduleProperties,
  id: new Long(8136728577, 70289336),
  description: "automatic SQL Stats compaction",
  username: "node",
  status: "succeeded",
};

const failedScheduleFixture = {
  ...defaultScheduleProperties,
  id: new Long(7003330561, 70312826),
  description:
    "ALTER TABLE movr.public.user_promo_codes ADD FOREIGN KEY (city, user_id) REFERENCES movr.public.users (city, id)",
  status: "failed",
  error: "mock failure message",
};

const canceledScheduleFixture = {
  ...defaultScheduleProperties,
  id: new Long(7002707969, 70312826),
  description: "Unspecified",
  status: "canceled",
};

const pausedScheduleFixture = {
  ...defaultScheduleProperties,
  id: new Long(6091954177, 70312826),
  description:
    "BACKUP DATABASE bank TO 'gs://acme-co-backup/database-bank-2017-03-29-nightly' AS OF SYSTEM TIME '-10s' INCREMENTAL FROM 'gs://acme-co-backup/database-bank-2017-03-27-weekly', 'gs://acme-co-backup/database-bank-2017-03-28-nightly' WITH revision_history\n",
  status: "paused",
};

const runningScheduleFixture = {
  ...defaultScheduleProperties,
  id: new Long(3390625793, 70312826),
  description: "GC for DROP TABLE havent_started_running",
  status: "running",
  fraction_completed: 0,
};

const runningWithMessageScheduleFixture = {
  ...defaultScheduleProperties,
  id: new Long(3390625793, 70312826),
  description: "GC for DROP TABLE no_duration_has_running_status",
  status: "running",
  fraction_completed: 0,
  running_status: "Waiting For GC TTL",
};

const runningWithRemainingScheduleFixture = {
  ...defaultScheduleProperties,
  id: new Long(6093756417, 70312826),
  description: "RESTORE data.* FROM $1 WITH OPTIONS (into_db='data2')",
  status: "running",
  fraction_completed: 0.38,
};

const runningWithMessageRemainingScheduleFixture = {
  ...defaultScheduleProperties,
  id: new Long(3390625793, 70312826),
  description: "automatic Span Config reconciliation",
  status: "running",
  fraction_completed: 0.66,
  running_status: "performing garbage collection on index 2",
};

export const retryRunningScheduleFixture = {
  ...defaultScheduleProperties,
  id: new Long(3390625793, 70312826),
  description: "GC for DROP TABLE havent_started_running_2",
  status: "retry-running",
  fraction_completed: 0,
  next_run: new protos.google.protobuf.Timestamp({
    seconds: new Long(3034648417), // some long time in the future, because it needs to be in the future to show as retrying
    nanos: 116912000,
  }),
  num_runs: new Long(3),
};

const retryRunningWithMessageScheduleFixture = {
  ...defaultScheduleProperties,
  id: new Long(3390625793, 70312826),
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

const retryRunningWithRemainingScheduleFixture = {
  ...defaultScheduleProperties,
  id: new Long(3390625793, 70312826),
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

const retryRunningWithMessageRemainingScheduleFixture = {
  ...defaultScheduleProperties,
  id: new Long(3390625793, 70312826),
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

const pendingScheduleFixture = {
  ...defaultScheduleProperties,
  id: new Long(5247850497, 70312826),
  description:
    "IMPORT PGDUMP 'userfile://defaultdb.public.userfiles_root/db.sql' WITH max_row_size='524288'",
  status: "pending",
};

const revertingScheduleFixture = {
  ...defaultScheduleProperties,
  id: new Long(5246539777, 70312826),
  description: "CREATE CHANGEFEED FOR foo WITH updated, resolved, diff",
  status: "reverting",
};

const retryRevertingScheduleFixture = {
  ...defaultScheduleProperties,
  id: new Long(3390625793, 70312826),
  description: "ALTER TABLE db.t ADD COLUMN b INT DEFAULT 1",
  status: "retry-reverting",
  next_run: new protos.google.protobuf.Timestamp({
    seconds: new Long(3034648417), // some long time in the future, because it needs to be in the future to show as retrying
    nanos: 116912000,
  }),
  num_runs: new Long(2),
};

const highwaterScheduleFixture = {
  ...defaultScheduleProperties,
  id: new Long(3390625793, 70312826),
  description: "ALTER TYPE status ADD VALUE 'pending';",
  status: "running",
  highwater_timestamp: new protos.google.protobuf.Timestamp({
    seconds: new Long(1634648117),
    nanos: 98294000,
  }),
  highwater_decimal: "test highwater decimal",
};

const cancelRequestedScheduleFixture = {
  ...defaultScheduleProperties,
  id: new Long(4337653761, 70312826),
  description: "SELECT schedule_id, schedule_type FROM [SHOW JOB 1]",
  status: "cancel-requested",
};

const pauseRequestedScheduleFixture = {
  ...defaultScheduleProperties,
  id: new Long(4338669569, 70312826),
  description: "SELECT schedule_id, schedule_type FROM [SHOW AUTOMATIC JOBS];",
  status: "pause-requested",
};

const revertFailedScheduleFixture = {
  ...defaultScheduleProperties,
  id: new Long(3391379457, 70312826),
  description: "GC for DROP DATABASE t CASCADE",
  status: "revert-failed",
};

export const allSchedulesFixture = [
  succeededScheduleFixture,
  failedScheduleFixture,
  canceledScheduleFixture,
  pausedScheduleFixture,
  runningScheduleFixture,
  runningWithMessageScheduleFixture,
  runningWithRemainingScheduleFixture,
  runningWithMessageRemainingScheduleFixture,
  retryRunningScheduleFixture,
  retryRunningWithMessageScheduleFixture,
  retryRunningWithRemainingScheduleFixture,
  retryRunningWithMessageRemainingScheduleFixture,
  pendingScheduleFixture,
  revertingScheduleFixture,
  retryRevertingScheduleFixture,
  highwaterScheduleFixture,
  cancelRequestedScheduleFixture,
  pauseRequestedScheduleFixture,
  revertFailedScheduleFixture,
];

const history = createMemoryHistory({ initialEntries: ["/statements"] });

const staticScheduleProps: Pick<
  SchedulesPageProps,
  | "history"
  | "location"
  | "match"
  | "sort"
  | "status"
  | "show"
  | "setSort"
  | "setStatus"
  | "setShow"
  | "refreshSchedules"
> = {
  history,
  location: {
    pathname: "/schedules",
    search: "",
    hash: "",
    state: null,
  },
  match: {
    path: "/schedules",
    url: "/schedules",
    isExact: true,
    params: "{}",
  },
  sort: {
    columnTitle: "creationTime",
    ascending: false,
  },
  status: "",
  show: "50",
  setSort: () => {},
  setStatus: () => {},
  setShow: () => {},
  refreshSchedules: () => null,
};

export const earliestRetainedTime = new protos.google.protobuf.Timestamp({
  seconds: new Long(1633611318),
  nanos: 200459000,
});

const getSchedulesPageProps = (
  schedules: Array<Schedule>,
  error: Error | null = null,
  loading = false,
): SchedulesPageProps => ({
  ...staticScheduleProps,
  schedules: SchedulesResponse.create({
    schedules: schedules,
    earliest_retained_time: earliestRetainedTime,
  }),
  schedulesError: error,
  schedulesLoading: loading,
});

export const withData: SchedulesPageProps =
  getSchedulesPageProps(allSchedulesFixture);
export const empty: SchedulesPageProps = getSchedulesPageProps([]);
export const loading: SchedulesPageProps = getSchedulesPageProps(
  allSchedulesFixture,
  null,
  true,
);
export const error: SchedulesPageProps = getSchedulesPageProps(
  allSchedulesFixture,
  new Error(schedulesTimeoutErrorMessage),
  false,
);
