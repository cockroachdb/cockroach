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
import Long from "long";

export const jobTablePropsFixture: JobTableProps = {
  sort: {
    sortKey: 3,
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
      jobs: [
        {
          id: new Long(8136728577, 70289336),
          type: "AUTO SQL STATS COMPACTION",
          description: "automatic SQL Stats compaction",
          username: "node",
          status: "succeeded",
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
        },
        {
          id: new Long(7003330561, 70312826),
          type: "SCHEMA CHANGE",
          description:
            "ALTER TABLE movr.public.user_promo_codes ADD FOREIGN KEY (city, user_id) REFERENCES movr.public.users (city, id)",
          username: "root",
          descriptor_ids: [58],
          status: "failed",
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
          error: "mock failure message",
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
        },
        {
          id: new Long(7002707969, 70312826),
          type: "UNSPECIFIED",
          description: "Unspecified",
          username: "root",
          descriptor_ids: [53],
          status: "canceled",
          created: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648118),
            nanos: 200459000,
          }),
          started: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648118),
            nanos: 215515000,
          }),
          finished: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648118),
            nanos: 220674000,
          }),
          modified: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648118),
            nanos: 219907000,
          }),
          fraction_completed: 1,
          last_run: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648118),
            nanos: 215515000,
          }),
          next_run: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648118),
            nanos: 215515100,
          }),
          num_runs: new Long(1),
        },
        {
          id: new Long(6091954177, 70312826),
          type: "BACKUP",
          description:
            "BACKUP DATABASE bank TO 'gs://acme-co-backup/database-bank-2017-03-29-nightly' AS OF SYSTEM TIME '-10s' INCREMENTAL FROM 'gs://acme-co-backup/database-bank-2017-03-27-weekly', 'gs://acme-co-backup/database-bank-2017-03-28-nightly' WITH revision_history\n",
          username: "root",
          descriptor_ids: [55],
          status: "paused",
          created: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 917066000,
          }),
          started: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 936238000,
          }),
          finished: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 941404000,
          }),
          modified: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 940848000,
          }),
          fraction_completed: 1,
          last_run: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 936238000,
          }),
          next_run: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 936238100,
          }),
          num_runs: new Long(1),
        },
        {
          id: new Long(3390625793, 70312826),
          type: "AUTO SPAN CONFIG RECONCILIATION",
          description: "GC for DROP TABLE havent_started_running",
          username: "root",
          descriptor_ids: [53],
          status: "running",
          created: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 98294000,
          }),
          started: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 116912000,
          }),
          finished: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 121906000,
          }),
          modified: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 121173000,
          }),
          fraction_completed: 0,
          last_run: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 116912000,
          }),
          next_run: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 116912100,
          }),
          num_runs: new Long(1),
        },
        {
          id: new Long(3390625793, 70312826),
          type: "AUTO SPAN CONFIG RECONCILIATION",
          description: "GC for DROP TABLE no_duration_has_running_status",
          username: "root",
          descriptor_ids: [53],
          status: "running",
          created: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 98294000,
          }),
          started: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 116912000,
          }),
          finished: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 121906000,
          }),
          modified: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 121173000,
          }),
          fraction_completed: 0,
          running_status: "Waiting For GC TTL",
          last_run: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 116912000,
          }),
          next_run: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 116912100,
          }),
          num_runs: new Long(1),
        },
        {
          id: new Long(6093756417, 70312826),
          type: "RESTORE",
          description: "RESTORE data.* FROM $1 WITH OPTIONS (into_db='data2')",
          username: "root",
          descriptor_ids: [56],
          status: "running",
          created: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 917066000,
          }),
          started: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 936268000,
          }),
          finished: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648118),
            nanos: 154016000,
          }),
          modified: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648118),
            nanos: 153272000,
          }),
          fraction_completed: 0.38,
          last_run: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 936268000,
          }),
          num_runs: new Long(1),
        },

        {
          id: new Long(3390625793, 70312826),
          type: "AUTO SPAN CONFIG RECONCILIATION",
          description: "automatic Span Config reconciliation",
          username: "root",
          descriptor_ids: [53],
          status: "running",
          created: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 98294000,
          }),
          started: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 116912000,
          }),
          finished: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 121906000,
          }),
          modified: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 121173000,
          }),
          fraction_completed: 0.66,
          running_status: "performing garbage collection on index 2",
          last_run: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 116912000,
          }),
          next_run: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 116912100,
          }),
          num_runs: new Long(1),
        },
        {
          id: new Long(3390625793, 70312826),
          type: "STREAM INGESTION",
          description: "GC for DROP TABLE havent_started_running_2",
          username: "root",
          descriptor_ids: [53],
          status: "retry-running",
          created: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 98294000,
          }),
          started: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 116912000,
          }),
          finished: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 121906000,
          }),
          modified: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 121173000,
          }),
          fraction_completed: 0,
          last_run: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 116912000,
          }),
          next_run: new protos.google.protobuf.Timestamp({
            seconds: new Long(3034648417), // some long time in the future, because it needs to be in the future to show as retrying
            nanos: 116912000,
          }),
          num_runs: new Long(3),
        },
        {
          id: new Long(3390625793, 70312826),
          type: "AUTO SPAN CONFIG RECONCILIATION",
          description: "GC for DROP TABLE no_duration_has_running_status",
          username: "root",
          descriptor_ids: [53],
          status: "retry-running",
          created: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 98294000,
          }),
          started: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 116912000,
          }),
          finished: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 121906000,
          }),
          modified: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 121173000,
          }),
          fraction_completed: 0,
          running_status: "Waiting For GC TTL",
          last_run: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 116912000,
          }),
          next_run: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 116912100,
          }),
          num_runs: new Long(1),
        },
        {
          id: new Long(3390625793, 70312826),
          type: "STREAM INGESTION",
          description:
            "RESTORE DATABASE backup_database_name FROM 'your_backup_location';",
          username: "root",
          descriptor_ids: [53],
          status: "retry-running",
          created: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 98294000,
          }),
          started: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 116912000,
          }),
          finished: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 121906000,
          }),
          modified: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 121173000,
          }),
          fraction_completed: 0.11,
          last_run: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 116912000,
          }),
          next_run: new protos.google.protobuf.Timestamp({
            seconds: new Long(3034648417), // some long time in the future, because it needs to be in the future to show as retrying
            nanos: 116912000,
          }),
          num_runs: new Long(3),
        },
        {
          id: new Long(3390625793, 70312826),
          type: "MIGRATION",
          description:
            "IMPORT MYSQLDUMP 'https://s3-us-west-1.amazonaws.com/cockroachdb-movr/datasets/employees-db/mysqldump/employees-full.sql.gz';",
          username: "root",
          descriptor_ids: [53],
          status: "retry-running",
          created: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 98294000,
          }),
          started: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 116912000,
          }),
          finished: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 121906000,
          }),
          modified: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 121173000,
          }),
          fraction_completed: 0.82,
          running_status: "performing garbage collection on table crl.roachers",
          last_run: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 116912000,
          }),
          next_run: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 116912100,
          }),
          num_runs: new Long(1),
        },
        {
          id: new Long(5247850497, 70312826),
          type: "IMPORT",
          description:
            "IMPORT PGDUMP 'userfile://defaultdb.public.userfiles_root/db.sql' WITH max_row_size='524288'",
          username: "root",
          descriptor_ids: [55],
          status: "pending",
          created: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 664059000,
          }),
          started: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 679202000,
          }),
          finished: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 883734000,
          }),
          modified: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 882945000,
          }),
          fraction_completed: 1,
          running_status: "synthetic retryable error",
          last_run: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 679202000,
          }),
          next_run: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 679202100,
          }),
          num_runs: new Long(1),
        },
        {
          id: new Long(5246539777, 70312826),
          type: "CHANGEFEED",
          description: "CREATE CHANGEFEED FOR foo WITH updated, resolved, diff",
          username: "root",
          descriptor_ids: [54],
          status: "reverting",
          created: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 664059000,
          }),
          started: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 679221000,
          }),
          finished: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 683950000,
          }),
          modified: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 683189000,
          }),
          fraction_completed: 1,
          last_run: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 679221000,
          }),
          next_run: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 679221100,
          }),
          num_runs: new Long(1),
        },
        {
          id: new Long(3390625793, 70312826),
          type: "NEW SCHEMA CHANGE",
          description: "ALTER TABLE db.t ADD COLUMN b INT DEFAULT 1",
          username: "root",
          descriptor_ids: [53],
          status: "retry-reverting",
          created: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 98294000,
          }),
          started: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 116912000,
          }),
          finished: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 121906000,
          }),
          modified: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 121173000,
          }),
          fraction_completed: 1,
          last_run: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 116912000,
          }),
          next_run: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 116912100,
          }),
          num_runs: new Long(1),
        },
        {
          id: new Long(3390625793, 70312826),
          type: "TYPEDESC SCHEMA CHANGE",
          description: "ALTER TYPE status ADD VALUE 'pending';",
          username: "root",
          descriptor_ids: [53],
          status: "running",
          highwater_timestamp: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 98294000,
          }),
          highwater_decimal: "test highwater decimal",
          created: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 98294000,
          }),
          started: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 116912000,
          }),
          finished: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 121906000,
          }),
          modified: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 121173000,
          }),
          fraction_completed: 1,
          last_run: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 116912000,
          }),
          next_run: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 116912100,
          }),
          num_runs: new Long(1),
        },
        {
          id: new Long(4337653761, 70312826),
          type: "CREATE STATS",
          description: "SELECT job_id, job_type FROM [SHOW JOB 1]",
          username: "root",
          descriptor_ids: [53],
          status: "cancel-requested",
          created: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 387146000,
          }),
          started: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 401320000,
          }),
          finished: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 405461000,
          }),
          modified: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 404765000,
          }),
          fraction_completed: 1,
          last_run: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 401320000,
          }),
          next_run: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 401320100,
          }),
          num_runs: new Long(1),
        },
        {
          id: new Long(4338669569, 70312826),
          type: "AUTO CREATE STATS",
          description: "SELECT job_id, job_type FROM [SHOW AUTOMATIC JOBS];",
          username: "root",
          descriptor_ids: [55],
          status: "pause-requested",
          created: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 387146000,
          }),
          started: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 401344000,
          }),
          finished: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 600051000,
          }),
          modified: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 599417000,
          }),
          fraction_completed: 1,
          last_run: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 401344000,
          }),
          next_run: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 401344100,
          }),
          num_runs: new Long(1),
        },
        {
          id: new Long(3391379457, 70312826),
          type: "SCHEMA CHANGE GC",
          description: "GC for DROP DATABASE t CASCADE",
          username: "root",
          descriptor_ids: [54],
          status: "revert-failed",
          created: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 98294000,
          }),
          started: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 116912000,
          }),
          finished: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 311634000,
          }),
          modified: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 310671000,
          }),
          fraction_completed: 1,
          last_run: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 116912000,
          }),
          next_run: new protos.google.protobuf.Timestamp({
            seconds: new Long(1634648117),
            nanos: 116912100,
          }),
          num_runs: new Long(1),
        },
      ],
    }),
  },
};
