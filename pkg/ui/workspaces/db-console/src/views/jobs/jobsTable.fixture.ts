// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { JobsTableProps, mapDispatchToProps } from "./index";
import { RouteComponentProps } from "react-router-dom";
import moment from "moment";
import * as protos from "@cockroachlabs/crdb-protobuf-client";
import { cockroach } from "src/js/protos";
import JobsResponse = cockroach.server.serverpb.JobsResponse;
import Long from "long";
import { createMemoryHistory } from "history";

const history = createMemoryHistory({ initialEntries: ["/jobs"] });

// export const timestamp = new protos.google.protobuf.Timestamp({
//   seconds: new Long(Date.parse("Sep 15 2021 01:00:00 GMT") * 1e-3),
// });

export const jobsTablePropsFixture: JobsTableProps & RouteComponentProps = {
  history,
  location: {
    pathname: "/jobs",
    search: "",
    hash: "",
    state: null,
  },
  match: { path: "/jobs", url: "/jobs", isExact: true, params: {} },
  // sort: {
  //   sortKey: 3,
  //   ascending: false,
  // },
  // status: "",
  // show: "50",
  // type: 0,
  // ...mapDispatchToProps,
  // setSort: (() => {}) as any,
  // setStatus: (() => {}) as any,
  // setShow: (() => {}) as any,
  // setType: (() => {}) as any,
  refreshJobs: (() => {}) as any,

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
          fraction_completed: 1,
        },
        {
          id: new Long(7002707969, 70312826),
          type: "UNSPECIFIED",
          description:
            "Unspecified",
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
        },
        {
          id: new Long(6093756417, 70312826),
          type: "RESTORE",
          description:
            "RESTORE data.* FROM $1 WITH OPTIONS (into_db='data2')",
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
        },
        {
          id: new Long(4337653761, 70312826),
          type: "CREATE STATS",
          description:
            "SELECT job_id, job_type FROM [SHOW JOB 1]",
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
        },
        {
          id: new Long(4338669569, 70312826),
          type: "AUTO CREATE STATS",
          description:
            "FIXME. Automatic table statistics jobs are not displayed even when the Type menu is set to All. To view these jobs, set Type to Automatic-Statistics Creation as described above.",
          //SELECT job_id, job_type FROM [SHOW AUTOMATIC JOBS];
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
        },
        {
          id: new Long(3391379457, 70312826),
          type: "SCHEMA CHANGE GC",
          description:
            "GC for DROP DATABASE t CASCADE",
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
        },
        {
          id: new Long(3390625793, 70312826),
          type: "TYPEDESC SCHEMA CHANGE",
          description:
            "ALTER TYPE status ADD VALUE 'pending';",
          username: "root",
          descriptor_ids: [53],
          status: "succeeded",
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
        },
        {
          id: new Long(3390625793, 70312826),
          type: "STREAM INGESTION",
          description:
            "RESTORE DATABASE backup_database_name FROM 'your_backup_location';",
          username: "root",
          descriptor_ids: [53],
          status: "succeeded",
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
        },
        {
          id: new Long(3390625793, 70312826),
          type: "NEW SCHEMA CHANGE",
          description:
            "ALTER TABLE db.t ADD COLUMN b INT DEFAULT 1",
          username: "root",
          descriptor_ids: [53],
          status: "succeeded",
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
        },
        {
          id: new Long(3390625793, 70312826),
          type: "MIGRATION",
          description:
            "IMPORT MYSQLDUMP 'https://s3-us-west-1.amazonaws.com/cockroachdb-movr/datasets/employees-db/mysqldump/employees-full.sql.gz';",
          username: "root",
          descriptor_ids: [53],
          status: "succeeded",
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
        },
        {
          id: new Long(3390625793, 70312826),
          type: "AUTO SPAN CONFIG RECONCILIATION",
          description:
            "auto span config reconciliation",
          username: "root",
          descriptor_ids: [53],
          status: "succeeded",
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
        },
      ],
    }),
  },
};
