// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { StatementsPageProps } from "@cockroachlabs/cluster-ui";
import { createMemoryHistory } from "history";
import Long from "long";
import * as protos from "src/js/protos";
import {
  refreshStatementDiagnosticsRequests,
  refreshStatements,
} from "src/redux/apiReducers";
type IStatementStatistics = protos.cockroach.sql.IStatementStatistics;

const history = createMemoryHistory({ initialEntries: ["/statements"] });

const statementStats: IStatementStatistics = {
  count: Long.fromNumber(180000),
  first_attempt_count: Long.fromNumber(50000),
  max_retries: Long.fromNumber(10),
  num_rows: {
    mean: 1,
    squared_diffs: 0,
  },
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

const statementsPagePropsFixture: StatementsPageProps = {
  history,
  location: {
    pathname: "/statements",
    search: "",
    hash: "",
    state: null,
  },
  databases: ["defaultdb", "foo"],
  nodeRegions: {
    "1": "gcp-us-east1",
    "2": "gcp-us-east1",
    "3": "gcp-us-west1",
    "4": "gcp-europe-west1",
  },
  columns: null,
  match: {
    path: "/statements",
    url: "/statements",
    isExact: true,
    params: {},
  },
  statements: [
    {
      label:
        "SELECT IFNULL(a, b) FROM (SELECT (SELECT code FROM promo_codes WHERE code > $1 ORDER BY code LIMIT _) AS a, (SELECT code FROM promo_codes ORDER BY code LIMIT _) AS b)",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label: "INSERT INTO vehicles VALUES ($1, $2, __more6__)",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label:
        "SELECT IFNULL(a, b) FROM (SELECT (SELECT id FROM users WHERE (city = $1) AND (id > $2) ORDER BY id LIMIT _) AS a, (SELECT id FROM users WHERE city = $1 ORDER BY id LIMIT _) AS b)",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label:
        "UPSERT INTO vehicle_location_histories VALUES ($1, $2, now(), $3, $4)",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label: "INSERT INTO user_promo_codes VALUES ($1, $2, $3, now(), _)",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label: "SELECT city, id FROM vehicles WHERE city = $1",
      implicitTxn: true,
      fullScan: true,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label:
        "INSERT INTO rides VALUES ($1, $2, $2, $3, $4, $5, _, now(), _, $6)",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label:
        "SELECT IFNULL(a, b) FROM (SELECT (SELECT id FROM vehicles WHERE (city = $1) AND (id > $2) ORDER BY id LIMIT _) AS a, (SELECT id FROM vehicles WHERE city = $1 ORDER BY id LIMIT _) AS b)",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label:
        "UPDATE rides SET end_address = $3, end_time = now() WHERE (city = $1) AND (id = $2)",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label: "INSERT INTO users VALUES ($1, $2, __more3__)",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label:
        "SELECT count(*) FROM user_promo_codes WHERE ((city = $1) AND (user_id = $2)) AND (code = $3)",
      implicitTxn: true,
      fullScan: true,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label: "INSERT INTO promo_codes VALUES ($1, $2, __more3__)",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label: "ALTER TABLE users SCATTER FROM (_, _) TO (_, _)",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label:
        "ALTER TABLE rides ADD FOREIGN KEY (vehicle_city, vehicle_id) REFERENCES vehicles (city, id)",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label: "SHOW database",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label:
        "CREATE TABLE IF NOT EXISTS promo_codes (code VARCHAR NOT NULL, description VARCHAR NULL, creation_time TIMESTAMP NULL, expiration_time TIMESTAMP NULL, rules JSONB NULL, PRIMARY KEY (code ASC))",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label: "ALTER TABLE users SPLIT AT VALUES (_, _)",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label: "ALTER TABLE vehicles SCATTER FROM (_, _) TO (_, _)",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label:
        "ALTER TABLE vehicle_location_histories ADD FOREIGN KEY (city, ride_id) REFERENCES rides (city, id)",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label:
        'CREATE TABLE IF NOT EXISTS user_promo_codes (city VARCHAR NOT NULL, user_id UUID NOT NULL, code VARCHAR NOT NULL, "timestamp" TIMESTAMP NULL, usage_count INT8 NULL, PRIMARY KEY (city ASC, user_id ASC, code ASC))',
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label: "INSERT INTO users VALUES ($1, $2, __more3__), (__more40__)",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label: "ALTER TABLE rides SCATTER FROM (_, _) TO (_, _)",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label: 'SET CLUSTER SETTING "cluster.organization" = $1',
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label:
        "ALTER TABLE vehicles ADD FOREIGN KEY (city, owner_id) REFERENCES users (city, id)",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label:
        "CREATE TABLE IF NOT EXISTS rides (id UUID NOT NULL, city VARCHAR NOT NULL, vehicle_city VARCHAR NULL, rider_id UUID NULL, vehicle_id UUID NULL, start_address VARCHAR NULL, end_address VARCHAR NULL, start_time TIMESTAMP NULL, end_time TIMESTAMP NULL, revenue DECIMAL(10,2) NULL, PRIMARY KEY (city ASC, id ASC), INDEX rides_auto_index_fk_city_ref_users (city ASC, rider_id ASC), INDEX rides_auto_index_fk_vehicle_city_ref_vehicles (vehicle_city ASC, vehicle_id ASC), CONSTRAINT check_vehicle_city_city CHECK (vehicle_city = city))",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label:
        "CREATE TABLE IF NOT EXISTS vehicles (id UUID NOT NULL, city VARCHAR NOT NULL, type VARCHAR NULL, owner_id UUID NULL, creation_time TIMESTAMP NULL, status VARCHAR NULL, current_location VARCHAR NULL, ext JSONB NULL, PRIMARY KEY (city ASC, id ASC), INDEX vehicles_auto_index_fk_city_ref_users (city ASC, owner_id ASC))",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label: "INSERT INTO rides VALUES ($1, $2, __more8__), (__more400__)",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label: "ALTER TABLE vehicles SPLIT AT VALUES (_, _)",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label: "SET sql_safe_updates = _",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label:
        "CREATE TABLE IF NOT EXISTS users (id UUID NOT NULL, city VARCHAR NOT NULL, name VARCHAR NULL, address VARCHAR NULL, credit_card VARCHAR NULL, PRIMARY KEY (city ASC, id ASC))",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label:
        'CREATE TABLE IF NOT EXISTS vehicle_location_histories (city VARCHAR NOT NULL, ride_id UUID NOT NULL, "timestamp" TIMESTAMP NOT NULL, lat FLOAT8 NULL, long FLOAT8 NULL, PRIMARY KEY (city ASC, ride_id ASC, "timestamp" ASC))',
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label: "SELECT * FROM crdb_internal.node_build_info",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label: "CREATE DATABASE movr",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label:
        "SELECT count(*) > _ FROM [SHOW ALL CLUSTER SETTINGS] AS _ (v) WHERE v = _",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label: 'SET CLUSTER SETTING "enterprise.license" = $1',
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label:
        "ALTER TABLE rides ADD FOREIGN KEY (city, rider_id) REFERENCES users (city, id)",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label:
        "ALTER TABLE user_promo_codes ADD FOREIGN KEY (city, user_id) REFERENCES users (city, id)",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label:
        "INSERT INTO promo_codes VALUES ($1, $2, __more3__), (__more900__)",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label: "ALTER TABLE rides SPLIT AT VALUES (_, _)",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label: "SELECT value FROM crdb_internal.node_build_info WHERE field = _",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label:
        "INSERT INTO vehicle_location_histories VALUES ($1, $2, __more3__), (__more900__)",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
    {
      label: "INSERT INTO vehicles VALUES ($1, $2, __more6__), (__more10__)",
      implicitTxn: true,
      fullScan: false,
      stats: statementStats,
      database: "defaultdb",
    },
  ],
  statementsError: null,
  apps: ["(internal)", "movr", "$ cockroach demo"],
  totalFingerprints: 95,
  lastReset: "2020-04-13 07:22:23",
  dismissAlertMessage: () => {},
  refreshStatementDiagnosticsRequests: (() => {}) as typeof refreshStatementDiagnosticsRequests,
  refreshStatements: (() => {}) as typeof refreshStatements,
  onActivateStatementDiagnostics: (_) => {},
  onDiagnosticsModalOpen: (_) => {},
  onPageChanged: (_) => {},
  resetSQLStats: () => {},
};

export default statementsPagePropsFixture;
