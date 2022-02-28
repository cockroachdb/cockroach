// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/* eslint-disable prettier/prettier */
import { StatementsPageProps } from "./statementsPage";
import moment from "moment";
import { createMemoryHistory } from "history";
import Long from "long";
import { noop } from "lodash";
import * as protos from "@cockroachlabs/crdb-protobuf-client";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { RequestError } from "src/util";

type IStatementDiagnosticsReport = cockroach.server.serverpb.IStatementDiagnosticsReport;
type IStatementStatistics = protos.cockroach.sql.IStatementStatistics;
type IExecStats = protos.cockroach.sql.IExecStats;

const history = createMemoryHistory({ initialEntries: ["/statements"] });

const execStats: Required<IExecStats> = {
  count: Long.fromNumber(180),
  network_bytes: {
    mean: 80,
    squared_diffs: 0.01,
  },
  max_mem_usage: {
    mean: 80,
    squared_diffs: 0.01,
  },
  contention_time: {
    mean: 80,
    squared_diffs: 0.01,
  },
  network_messages: {
    mean: 80,
    squared_diffs: 0.01,
  },
  max_disk_usage: {
    mean: 80,
    squared_diffs: 0.01,
  },
};

const statementStats: Required<IStatementStatistics> = {
  count: Long.fromNumber(180000),
  first_attempt_count: Long.fromNumber(50000),
  max_retries: Long.fromNumber(10),
  sql_type: "DDL",
  nodes: [Long.fromNumber(1), Long.fromNumber(2)],
  num_rows: {
    mean: 1,
    squared_diffs: 0,
  },
  legacy_last_err: "",
  legacy_last_err_redacted: "",
  parse_lat: {
    mean: 0,
    squared_diffs: 0,
  },
  plan_lat: {
    mean: 0.00018,
    squared_diffs: 5.4319999999999994e-9,
  },
  run_lat: {
    mean: 0.0022536666666666664,
    squared_diffs: 0.0000020303526666666667,
  },
  service_lat: {
    mean: 0.002496,
    squared_diffs: 0.000002308794,
  },
  overhead_lat: {
    mean: 0.00006233333333333315,
    squared_diffs: 5.786666666666667e-10,
  },
  bytes_read: {
    mean: 80,
    squared_diffs: 0.01,
  },
  rows_read: {
    mean: 10,
    squared_diffs: 1,
  },
  rows_written: {
    mean: 2,
    squared_diffs: 0.005,
  },
  plan_gists: ["Ais="],
  exec_stats: execStats,
  last_exec_timestamp: {
    seconds: Long.fromInt(1599670292),
    nanos: 111613000,
  },
  sensitive_info: {
    last_err: "",
    most_recent_plan_description: {
      name: "root",
      children: [
        {
          name: "render",
          attrs: [
            {
              key: "render",
              value: "COALESCE(a, b)",
            },
          ],
          children: [
            {
              name: "values",
              attrs: [
                {
                  key: "size",
                  value: "2 columns, 1 row",
                },
                {
                  key: "row 0, expr",
                  value:
                    "(SELECT code FROM promo_codes WHERE code > $1 ORDER BY code LIMIT _)",
                },
                {
                  key: "row 0, expr",
                  value: "(SELECT code FROM promo_codes ORDER BY code LIMIT _)",
                },
              ],
            },
          ],
        },
        {
          name: "subquery",
          attrs: [
            {
              key: "id",
              value: "@S1",
            },
            {
              key: "original sql",
              value:
                "(SELECT code FROM promo_codes WHERE code > $1 ORDER BY code LIMIT _)",
            },
            {
              key: "exec mode",
              value: "one row",
            },
          ],
          children: [
            {
              name: "scan",
              attrs: [
                {
                  key: "table",
                  value: "promo_codes@primary",
                },
                {
                  key: "spans",
                  value: "1 span",
                },
                {
                  key: "limit",
                  value: "1",
                },
              ],
            },
          ],
        },
        {
          name: "subquery",
          attrs: [
            {
              key: "id",
              value: "@S2",
            },
            {
              key: "original sql",
              value: "(SELECT code FROM promo_codes ORDER BY code LIMIT _)",
            },
            {
              key: "exec mode",
              value: "one row",
            },
          ],
          children: [
            {
              name: "scan",
              attrs: [
                {
                  key: "table",
                  value: "promo_codes@primary",
                },
                {
                  key: "spans",
                  value: "ALL",
                },
                {
                  key: "limit",
                  value: "1",
                },
              ],
            },
          ],
        },
      ],
    },
  },
};

const diagnosticsReports: IStatementDiagnosticsReport[] = [
  {
    id: Long.fromNumber(594413966918975489),
    completed: true,
    statement_fingerprint: "SHOW database",
    statement_diagnostics_id: Long.fromNumber(594413981435920385),
    requested_at: { seconds: Long.fromNumber(1601471146), nanos: 737251000 },
  },
  {
    id: Long.fromNumber(594413966918975429),
    completed: true,
    statement_fingerprint: "SHOW database",
    statement_diagnostics_id: Long.fromNumber(594413281435920385),
    requested_at: { seconds: Long.fromNumber(1601491146), nanos: 737251000 },
  },
];

const diagnosticsReportsInProgress: IStatementDiagnosticsReport[] = [
  {
    id: Long.fromNumber(594413966918975489),
    completed: false,
    statement_fingerprint: "SHOW database",
    statement_diagnostics_id: Long.fromNumber(594413981435920385),
    requested_at: { seconds: Long.fromNumber(1601471146), nanos: 737251000 },
  },
  {
    id: Long.fromNumber(594413966918975429),
    completed: true,
    statement_fingerprint: "SHOW database",
    statement_diagnostics_id: Long.fromNumber(594413281435920385),
    requested_at: { seconds: Long.fromNumber(1601491146), nanos: 737251000 },
  },
];

const aggregatedTs = Date.parse("Sep 15 2021 01:00:00 GMT") * 1e-3;
const aggregationInterval = 3600; // 1 hour

const statementsPagePropsFixture: StatementsPageProps = {
  history,
  location: {
    pathname: "/statements",
    search: "",
    hash: "",
    state: null,
  },
  match: {
    path: "/statements",
    url: "/statements",
    isExact: true,
    params: {},
  },
  databases: ["defaultdb", "foo", "system"],
  nodeRegions: {
    "1": "gcp-us-east1",
    "2": "gcp-us-east1",
    "3": "gcp-us-west1",
    "4": "gcp-europe-west1",
  },
  sortSetting: {
    ascending: false,
    columnTitle: "executionCount"
  },
  search: "",
  filters: {
    app: "",
    timeNumber: "0",
    timeUnit: "seconds",
    fullScan: false,
    sqlType: "",
    database: "",
    regions: "",
    nodes: "",
  },
  // Aggregate key values in these statements will need to change if implementation
  // of 'statementKey' in appStats.ts changes.
  statements: [
    {
      aggregatedFingerprintID: "1253500548539870016",
      label:
        "SELECT IFNULL(a, b) FROM (SELECT (SELECT code FROM promo_codes WHERE code > $1 ORDER BY code LIMIT _) AS a, (SELECT code FROM promo_codes ORDER BY code LIMIT _) AS b)",
      summary: "SELECT IFNULL(a, b) FROM (SELECT)",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: "1985666523427702831",
      label: "INSERT INTO vehicles VALUES ($1, $2, __more6__)",
      summary: "INSERT INTO vehicles",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: "13649565517143827225",
      label:
        "SELECT IFNULL(a, b) FROM (SELECT (SELECT id FROM users WHERE (city = $1) AND (id > $2) ORDER BY id LIMIT _) AS a, (SELECT id FROM users WHERE city = $1 ORDER BY id LIMIT _) AS b)",
      summary: "SELECT IFNULL(a, b) FROM (SELECT)",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: "1533636712988872414",
      label:
        "UPSERT INTO vehicle_location_histories VALUES ($1, $2, now(), $3, $4)",
      summary: "UPSERT INTO vehicle_location_histories",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: "2461578209191418170",
      label: "INSERT INTO user_promo_codes VALUES ($1, $2, $3, now(), _)",
      summary: "INSERT INTO user_promo_codes",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: "4705782015019656142",
      label: "SELECT city, id FROM vehicles WHERE city = $1",
      summary: "SELECT city, id FROM vehicles",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: true,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: "2298970482983227199",
      label:
        "INSERT INTO rides VALUES ($1, $2, $2, $3, $4, $5, _, now(), _, $6)",
      summary: "INSERT INTO rides",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: "4716433305747424413",
      label: "SELECT IFNULL(a, b) FROM (SELECT  AS a, AS b)",
      summary: "SELECT IFNULL(a, b) FROM (SELECT)",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: "367828504526856403",
      label:
        "UPDATE rides SET end_address = $3, end_time = now() WHERE (city = $1) AND (id = $2)",
      summary: "UPDATE rides SET end_address = $... WHERE (city = $1) AND...",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: "14972494059652918390",
      label: "INSERT INTO users VALUES ($1, $2, __more3__)",
      summary: "INSERT INTO users",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: "15897033026745880862",
      label:
        "SELECT count(*) FROM user_promo_codes WHERE ((city = $1) AND (user_id = $2)) AND (code = $3)",
      summary: "SELECT count(*) FROM user_promo_codes",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: true,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: '49958554803360403681',
      label: "INSERT INTO promo_codes VALUES ($1, $2, __more3__)",
      summary: "INSERT INTO promo_codes",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: '9233296116064220812',
      label: "ALTER TABLE users SCATTER FROM (_, _) TO (_, _)",
      summary: "ALTER TABLE users SCATTER FROM (_, _) TO (_, _)",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: '6117473345491440803',
      label:
        "ALTER TABLE rides ADD FOREIGN KEY (vehicle_city, vehicle_id) REFERENCES vehicles (city, id)",
      summary:
        "ALTER TABLE rides ADD FOREIGN KEY (vehicle_city, vehicle_id) REFERENCES vehicles (city, id)",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: '1301242584620444873',
      label: "SHOW database",
      summary: "SHOW database",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
      diagnosticsReports,
    },
    {
      aggregatedFingerprintID: '11195381626529102926',
      label:
        "CREATE TABLE IF NOT EXISTS promo_codes (code VARCHAR NOT NULL, description VARCHAR NULL, creation_time TIMESTAMP NULL, expiration_time TIMESTAMP NULL, rules JSONB NULL, PRIMARY KEY (code ASC))",
      summary:
        "CREATE TABLE IF NOT EXISTS promo_codes (code VARCHAR NOT NULL, description VARCHAR NULL, creation_time TIMESTAMP NULL, expiration_time TIMESTAMP NULL, rules JSONB NULL, PRIMARY KEY (code ASC))",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: '18127289707013477303',
      label: "ALTER TABLE users SPLIT AT VALUES (_, _)",
      summary: "ALTER TABLE users SPLIT AT VALUES (_, _)",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: '2499764450427976233',
      label: "ALTER TABLE vehicles SCATTER FROM (_, _) TO (_, _)",
      summary: "ALTER TABLE vehicles SCATTER FROM (_, _) TO (_, _)",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: '818321793552651414',
      label:
        "ALTER TABLE vehicle_location_histories ADD FOREIGN KEY (city, ride_id) REFERENCES rides (city, id)",
      summary:
        "ALTER TABLE vehicle_location_histories ADD FOREIGN KEY (city, ride_id) REFERENCES rides (city, id)",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: '13217779306501326587',
      label:
        'CREATE TABLE IF NOT EXISTS user_promo_codes (city VARCHAR NOT NULL, user_id UUID NOT NULL, code VARCHAR NOT NULL, "timestamp" TIMESTAMP NULL, usage_count INT8 NULL, PRIMARY KEY (city ASC, user_id ASC, code ASC))',
      summary:
        'CREATE TABLE IF NOT EXISTS user_promo_codes (city VARCHAR NOT NULL, user_id UUID NOT NULL, code VARCHAR NOT NULL, "timestamp" TIMESTAMP NULL, usage_count INT8 NULL, PRIMARY KEY (city ASC, user_id ASC, code ASC))',
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: '6325213731862855938',
      label: "INSERT INTO users VALUES ($1, $2, __more3__), (__more40__)",
      summary: "INSERT INTO users VALUES",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: '17372586739449521577',
      label: "ALTER TABLE rides SCATTER FROM (_, _) TO (_, _)",
      summary: "ALTER TABLE rides SCATTER FROM (_, _) TO (_, _)",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: '17098541896015126122',
      label: 'SET CLUSTER SETTING "cluster.organization" = $1',
      summary: 'SET CLUSTER SETTING "cluster.organization" = $1',
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: '13350023170184726428',
      label:
        "ALTER TABLE vehicles ADD FOREIGN KEY (city, owner_id) REFERENCES users (city, id)",
      summary:
        "ALTER TABLE vehicles ADD FOREIGN KEY (city, owner_id) REFERENCES users (city, id)",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: '2695725667586429780',
      label:
        "CREATE TABLE IF NOT EXISTS rides (id UUID NOT NULL, city VARCHAR NOT NULL, vehicle_city VARCHAR NULL, rider_id UUID NULL, vehicle_id UUID NULL, start_address VARCHAR NULL, end_address VARCHAR NULL, start_time TIMESTAMP NULL, end_time TIMESTAMP NULL, revenue DECIMAL(10,2) NULL, PRIMARY KEY (city ASC, id ASC), INDEX rides_auto_index_fk_city_ref_users (city ASC, rider_id ASC), INDEX rides_auto_index_fk_vehicle_city_ref_vehicles (vehicle_city ASC, vehicle_id ASC), CONSTRAINT check_vehicle_city_city CHECK (vehicle_city = city))",
      summary:
        "CREATE TABLE IF NOT EXISTS rides (id UUID NOT NULL, city VARCHAR NOT NULL, vehicle_city VARCHAR NULL, rider_id UUID NULL, vehicle_id UUID NULL, start_address VARCHAR NULL, end_address VARCHAR NULL, start_time TIMESTAMP NULL, end_time TIMESTAMP NULL, revenue DECIMAL(10,2) NULL, PRIMARY KEY (city ASC, id ASC), INDEX rides_auto_index_fk_city_ref_users (city ASC, rider_id ASC), INDEX rides_auto_index_fk_vehicle_city_ref_vehicles (vehicle_city ASC, vehicle_id ASC), CONSTRAINT check_vehicle_city_city CHECK (vehicle_city = city))",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: '6754865160812330169',
      label:
        "CREATE TABLE IF NOT EXISTS vehicles (id UUID NOT NULL, city VARCHAR NOT NULL, type VARCHAR NULL, owner_id UUID NULL, creation_time TIMESTAMP NULL, status VARCHAR NULL, current_location VARCHAR NULL, ext JSONB NULL, PRIMARY KEY (city ASC, id ASC), INDEX vehicles_auto_index_fk_city_ref_users (city ASC, owner_id ASC))",
      summary:
        "CREATE TABLE IF NOT EXISTS vehicles (id UUID NOT NULL, city VARCHAR NOT NULL, type VARCHAR NULL, owner_id UUID NULL, creation_time TIMESTAMP NULL, status VARCHAR NULL, current_location VARCHAR NULL, ext JSONB NULL, PRIMARY KEY (city ASC, id ASC), INDEX vehicles_auto_index_fk_city_ref_users (city ASC, owner_id ASC))",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: '6810471486115018510',
      label: "INSERT INTO rides VALUES ($1, $2, __more8__), (__more400__)",
      summary: "INSERT INTO rides",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: '13265908854908549668',
      label: "ALTER TABLE vehicles SPLIT AT VALUES (_, _)",
      summary: "ALTER TABLE vehicles SPLIT AT VALUES (_, _)",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: '18377382163116490400',
      label: "SET sql_safe_updates = _",
      summary: "SET sql_safe_updates = _",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: '8695470234690735168',
      label:
        "CREATE TABLE IF NOT EXISTS users (id UUID NOT NULL, city VARCHAR NOT NULL, name VARCHAR NULL, address VARCHAR NULL, credit_card VARCHAR NULL, PRIMARY KEY (city ASC, id ASC))",
      summary:
        "CREATE TABLE IF NOT EXISTS users (id UUID NOT NULL, city VARCHAR NOT NULL, name VARCHAR NULL, address VARCHAR NULL, credit_card VARCHAR NULL, PRIMARY KEY (city ASC, id ASC))",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: '9261848985398568228',
      label:
        'CREATE TABLE IF NOT EXISTS vehicle_location_histories (city VARCHAR NOT NULL, ride_id UUID NOT NULL, "timestamp" TIMESTAMP NOT NULL, lat FLOAT8 NULL, long FLOAT8 NULL, PRIMARY KEY (city ASC, ride_id ASC, "timestamp" ASC))',
      summary:
        'CREATE TABLE IF NOT EXISTS vehicle_location_histories (city VARCHAR NOT NULL, ride_id UUID NOT NULL, "timestamp" TIMESTAMP NOT NULL, lat FLOAT8 NULL, long FLOAT8 NULL, PRIMARY KEY (city ASC, ride_id ASC, "timestamp" ASC))',
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: '4176684928840388768',
      label: "SELECT * FROM crdb_internal.node_build_info",
      summary: "SELECT * FROM crdb_internal.node_build_info",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: "15868120298061590648",
      label: "CREATE DATABASE movr",
      summary: "CREATE DATABASE movr",
      implicitTxn: true,
      aggregatedTs,
      aggregationInterval,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
      diagnosticsReports: diagnosticsReportsInProgress,
    },
    {
      aggregatedFingerprintID: "13070583869906258880",
      label:
        "SELECT count(*) > _ FROM [SHOW ALL CLUSTER SETTINGS] AS _ (v) WHERE v = _",
      summary: "SELECT count(*) > _ FROM [SHOW ALL CLUSTER SETTINGS] AS...",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: "641287435601027145",
      label: 'SET CLUSTER SETTING "enterprise.license" = $1',
      summary: 'SET CLUSTER SETTING "enterprise.license" = $1',
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: "16743225271705059729",
      label:
        "ALTER TABLE rides ADD FOREIGN KEY (city, rider_id) REFERENCES users (city, id)",
      summary:
        "ALTER TABLE rides ADD FOREIGN KEY (city, rider_id) REFERENCES users (city, id)",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: "6075815909800602827",
      label:
        "ALTER TABLE user_promo_codes ADD FOREIGN KEY (city, user_id) REFERENCES users (city, id)",
      summary:
        "ALTER TABLE user_promo_codes ADD FOREIGN KEY (city, user_id) REFERENCES users (city, id)",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: "5158086166870396309",
      label:
        "INSERT INTO promo_codes VALUES ($1, $2, __more3__), (__more900__)",
      summary: "INSERT INTO promo_codes",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: "13494397675172244644",
      label: "ALTER TABLE rides SPLIT AT VALUES (_, _)",
      summary: "ALTER TABLE rides SPLIT AT VALUES (_, _)",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: "101921598584277094",
      label: "SELECT value FROM crdb_internal.node_build_info WHERE field = _",
      summary: "SELECT value FROM crdb_internal.node_build_info",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: "7880339715822034020",
      label:
        "INSERT INTO vehicle_location_histories VALUES ($1, $2, __more3__), (__more900__)",
      summary: "INSERT INTO vehicle_location_histories",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
    {
      aggregatedFingerprintID: "16819876564846676829",
      label: "INSERT INTO vehicles VALUES ($1, $2, __more6__), (__more10__)",
      summary: "INSERT INTO vehicles",
      aggregatedTs,
      aggregationInterval,
      implicitTxn: true,
      database: "defaultdb",
      fullScan: false,
      stats: statementStats,
    },
  ],
  statementsError: null,
  timeScale: {
    windowSize: moment.duration(5, "day"),
    sampleSize: moment.duration(5, "minutes"),
    fixedWindowEnd: moment.utc("2021.12.12"),
    key: "Custom"
  },
  apps: ["$ internal", "movr", "$ cockroach demo"],
  totalFingerprints: 95,
  lastReset: "2020-04-13 07:22:23",
  columns: null,
  isTenant: false,
  hasViewActivityRedactedRole: false,
  dismissAlertMessage: noop,
  refreshStatementDiagnosticsRequests: noop,
  refreshStatements: noop,
  refreshUserSQLRoles: noop,
  resetSQLStats: noop,
  onTimeScaleChange: noop,
  onActivateStatementDiagnostics: noop,
  onDiagnosticsModalOpen: noop,
  onSearchComplete: noop,
  onSelectDiagnosticsReportDropdownOption: noop,
  onColumnsChange: noop,
  onSortingChange: noop,
  onFilterChange: noop,
};

export const statementsPagePropsWithRequestError: StatementsPageProps = {
  ...statementsPagePropsFixture,
  statementsError: new RequestError(
    "request_error",
    403,
    "this operation requires admin privilege",
  ),
};

export default statementsPagePropsFixture;
