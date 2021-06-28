// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/* eslint-disable @typescript-eslint/camelcase */
import { createMemoryHistory } from "history";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import Long from "long";

const history = createMemoryHistory({ initialEntries: ["/transactions"] });

export const routeProps = {
  history,
  location: {
    pathname: "/transactions",
    search: "",
    hash: "",
    // eslint-disable-next-line @typescript-eslint/ban-ts-ignore
    // @ts-ignore
    state: null,
  },
  match: {
    path: "/transactions",
    url: "/transactions",
    isExact: true,
    params: {},
  },
};

export const nodeRegions: { [nodeId: string]: string } = {
  "1": "gcp-us-east1",
  "2": "gcp-us-east1",
  "3": "gcp-us-west1",
  "4": "gcp-europe-west1",
};

export const data: cockroach.server.serverpb.IStatementsResponse = {
  statements: [
    {
      key: {
        key_data: {
          query:
            "UPDATE sqlliveness SET expiration = $1 WHERE session_id = $2 RETURNING session_id",
          app: "$ internal-update-session",
          distSQL: false,
          failed: false,
          opt: true,
          implicit_txn: false,
          vec: false,
        },
        node_id: 5,
      },
      stats: {
        count: Long.fromInt(557),
        nodes: [Long.fromNumber(1), Long.fromNumber(2)],
        first_attempt_count: Long.fromInt(557),
        max_retries: Long.fromInt(0),
        legacy_last_err: "",
        num_rows: { mean: 0, squared_diffs: 0 },
        parse_lat: { mean: 0, squared_diffs: 0 },
        plan_lat: {
          mean: 0.002615184919210055,
          squared_diffs: 0.28076569372595306,
        },
        run_lat: {
          mean: 0.026397836624775597,
          squared_diffs: 53.447023474370134,
        },
        service_lat: {
          mean: -9223372036.825768,
          squared_diffs: 61.00942964330534,
        },
        overhead_lat: {
          mean: -9223372036.854765,
          squared_diffs: 1.841144694481045e-7,
        },
        legacy_last_err_redacted: "",
        sensitive_info: {
          last_err: "",
          most_recent_plan_description: {
            name: "update",
            attrs: [
              { key: "table", value: "sqlliveness" },
              { key: "set", value: "expiration" },
            ],
            children: [
              {
                name: "render",
                children: [
                  {
                    name: "scan",
                    attrs: [
                      { key: "missing stats", value: "" },
                      {
                        key: "table",
                        value: "sqlliveness@primary",
                      },
                      { key: "spans", value: "1 span" },
                    ],
                  },
                ],
              },
            ],
          },
          most_recent_plan_timestamp: {
            seconds: Long.fromInt(1599670292),
            nanos: 111613000,
          },
        },
        bytes_read: {
          mean: 61.021543985637344,
          squared_diffs: 47.74147217235174,
        },
        rows_read: { mean: 1, squared_diffs: 0 },
      },
      id: Long.fromInt(100),
    },
    {
      key: {
        key_data: {
          query: "SELECT expiration FROM sqlliveness WHERE session_id = $1",
          app: "$ internal-fetch-single-session",
          distSQL: false,
          failed: false,
          opt: true,
          implicit_txn: false,
          vec: false,
        },
        node_id: 5,
      },
      stats: {
        count: Long.fromInt(70),
        nodes: [Long.fromNumber(1), Long.fromNumber(3)],
        first_attempt_count: Long.fromInt(70),
        max_retries: Long.fromInt(0),
        legacy_last_err: "",
        num_rows: { mean: 0, squared_diffs: 0 },
        parse_lat: { mean: 0, squared_diffs: 0 },
        plan_lat: {
          mean: 0.0009092285714285716,
          squared_diffs: 0.00011917436234285714,
        },
        run_lat: {
          mean: 0.06882445714285716,
          squared_diffs: 20.155833230423376,
        },
        service_lat: {
          mean: -9223372036.785038,
          squared_diffs: 20.17413523535288,
        },
        overhead_lat: {
          mean: -9223372036.854773,
          squared_diffs: 6.039044819772243e-10,
        },
        legacy_last_err_redacted: "",
        sensitive_info: {
          last_err: "",
          most_recent_plan_description: {
            name: "scan",
            attrs: [
              { key: "missing stats", value: "" },
              {
                key: "table",
                value: "sqlliveness@primary",
              },
              { key: "spans", value: "1 span" },
            ],
          },
          most_recent_plan_timestamp: {
            seconds: Long.fromInt(1599670162),
            nanos: 750239000,
          },
        },
        bytes_read: {
          mean: 61.11428571428572,
          squared_diffs: 31.085714285714225,
        },
        rows_read: { mean: 1, squared_diffs: 0 },
      },
      id: Long.fromInt(101),
    },
    {
      key: {
        key_data: {
          query:
            'SELECT "descID", version, expiration FROM system.public.lease AS OF SYSTEM TIME _ WHERE "nodeID" = _',
          app: "$ internal-read orphaned leases",
          distSQL: false,
          failed: false,
          opt: true,
          implicit_txn: true,
          vec: false,
        },
        node_id: 5,
      },
      stats: {
        count: Long.fromInt(1),
        nodes: [Long.fromNumber(1), Long.fromNumber(3)],
        first_attempt_count: Long.fromInt(1),
        max_retries: Long.fromInt(0),
        legacy_last_err: "",
        num_rows: { mean: 0, squared_diffs: 0 },
        parse_lat: { mean: 0.008484, squared_diffs: 0 },
        plan_lat: { mean: 0.010116, squared_diffs: 0 },
        run_lat: { mean: 0.008816, squared_diffs: 0 },
        service_lat: { mean: 0.027419, squared_diffs: 0 },
        overhead_lat: { mean: 0.0000030000000000030003, squared_diffs: 0 },
        legacy_last_err_redacted: "",
        sensitive_info: {
          last_err: "",
          most_recent_plan_description: {
            name: "filter",
            attrs: [{ key: "filter", value: '"nodeID" = _' }],
            children: [
              {
                name: "scan",
                attrs: [
                  { key: "missing stats", value: "" },
                  {
                    key: "table",
                    value: "lease@primary",
                  },
                  { key: "spans", value: "FULL SCAN" },
                ],
              },
            ],
          },
          most_recent_plan_timestamp: {
            seconds: Long.fromInt(1599667573),
            nanos: 295410000,
          },
        },
        bytes_read: { mean: 40, squared_diffs: 0 },
        rows_read: { mean: 1, squared_diffs: 0 },
      },
      id: Long.fromInt(102),
    },
    {
      key: {
        key_data: {
          query:
            "UPDATE system.jobs SET claim_session_id = _ WHERE (claim_session_id != $1) AND (NOT crdb_internal.sql_liveness_is_alive(claim_session_id))",
          app: "$ internal-expire-sessions",
          distSQL: false,
          failed: false,
          opt: true,
          implicit_txn: true,
          vec: false,
        },
        node_id: 5,
      },
      stats: {
        count: Long.fromInt(280),
        nodes: [Long.fromNumber(3), Long.fromNumber(4)],
        first_attempt_count: Long.fromInt(280),
        max_retries: Long.fromInt(0),
        legacy_last_err: "",
        num_rows: { mean: 0, squared_diffs: 0 },
        parse_lat: { mean: 0, squared_diffs: 0 },
        plan_lat: {
          mean: 0.008967746428571436,
          squared_diffs: 0.6313926630769965,
        },
        run_lat: {
          mean: 0.023127346428571432,
          squared_diffs: 24.480558092911405,
        },
        service_lat: {
          mean: -9223372036.82268,
          squared_diffs: 32.03454110201346,
        },
        overhead_lat: {
          mean: -9223372036.854774,
          squared_diffs: 2.750311978161335e-9,
        },
        legacy_last_err_redacted: "",
        sensitive_info: {
          last_err: "",
          most_recent_plan_description: {
            name: "update",
            attrs: [
              { key: "table", value: "jobs" },
              {
                key: "set",
                value: "claim_session_id",
              },
              { key: "auto commit", value: "" },
            ],
            children: [
              {
                name: "render",
                children: [
                  {
                    name: "filter",
                    attrs: [
                      {
                        key: "filter",
                        value:
                          "(claim_session_id != _) AND (NOT crdb_internal.sql_liveness_is_alive(claim_session_id))",
                      },
                    ],
                    children: [
                      {
                        name: "scan",
                        attrs: [
                          { key: "missing stats", value: "" },
                          {
                            key: "table",
                            value: "jobs@primary",
                          },
                          { key: "spans", value: "FULL SCAN" },
                        ],
                      },
                    ],
                  },
                ],
              },
            ],
          },
          most_recent_plan_timestamp: {
            seconds: Long.fromInt(1599670292),
            nanos: 111319000,
          },
        },
        bytes_read: {
          mean: 10621.267857142862,
          squared_diffs: 470702848.9107132,
        },
        rows_read: {
          mean: 15.789285714285711,
          squared_diffs: 652.5678571428576,
        },
      },
      id: Long.fromInt(103),
    },
    {
      key: {
        key_data: {
          query: "INSERT INTO sqlliveness VALUES ($1, $2)",
          app: "$ internal-insert-session",
          distSQL: false,
          failed: false,
          opt: true,
          implicit_txn: false,
          vec: false,
        },
        node_id: 5,
      },
      stats: {
        count: Long.fromInt(1),
        nodes: [Long.fromNumber(2), Long.fromNumber(4)],
        first_attempt_count: Long.fromInt(1),
        max_retries: Long.fromInt(0),
        legacy_last_err: "",
        num_rows: { mean: 1, squared_diffs: 0 },
        parse_lat: { mean: 0, squared_diffs: 0 },
        plan_lat: { mean: 0.012858, squared_diffs: 0 },
        run_lat: { mean: 0.011222, squared_diffs: 0 },
        service_lat: { mean: -9223372036.830692, squared_diffs: 0 },
        overhead_lat: { mean: -9223372036.854773, squared_diffs: 0 },
        legacy_last_err_redacted: "",
        sensitive_info: {
          last_err: "",
          most_recent_plan_description: {
            name: "insert",
            attrs: [
              { key: "into", value: "sqlliveness(session_id, expiration)" },
            ],
            children: [
              {
                name: "values",
                attrs: [{ key: "size", value: "2 columns, 1 row" }],
              },
            ],
          },
          most_recent_plan_timestamp: {
            seconds: Long.fromInt(1599667573),
            nanos: 292763000,
          },
        },
        bytes_read: { mean: 0, squared_diffs: 0 },
        rows_read: { mean: 0, squared_diffs: 0 },
      },
      id: Long.fromInt(104),
    },
    {
      key: {
        key_data: {
          query: "SHOW CLUSTER SETTING version",
          app: "$ internal-show-version",
          distSQL: false,
          failed: false,
          opt: true,
          implicit_txn: true,
          vec: false,
        },
        node_id: 5,
      },
      stats: {
        count: Long.fromInt(1),
        nodes: [Long.fromNumber(1)],
        first_attempt_count: Long.fromInt(1),
        max_retries: Long.fromInt(0),
        legacy_last_err: "",
        num_rows: { mean: 0, squared_diffs: 0 },
        parse_lat: { mean: 0.000453, squared_diffs: 0 },
        plan_lat: { mean: 0.000099, squared_diffs: 0 },
        run_lat: { mean: 0.066172, squared_diffs: 0 },
        service_lat: { mean: 0.066726, squared_diffs: 0 },
        overhead_lat: { mean: 0.000002000000000002, squared_diffs: 0 },
        legacy_last_err_redacted: "",
        sensitive_info: {
          last_err: "",
          most_recent_plan_description: { name: "show" },
          most_recent_plan_timestamp: {
            seconds: Long.fromInt(1599667573),
            nanos: 343125000,
          },
        },
        bytes_read: { mean: 0, squared_diffs: 0 },
        rows_read: { mean: 0, squared_diffs: 0 },
      },
      id: Long.fromInt(105),
    },
    {
      key: {
        key_data: {
          query: "SELECT value FROM system.settings WHERE name = $1",
          app: "$ internal-read-setting",
          distSQL: false,
          failed: false,
          opt: true,
          implicit_txn: false,
          vec: false,
        },
        node_id: 5,
      },
      stats: {
        count: Long.fromInt(1),
        nodes: [Long.fromNumber(3), Long.fromNumber(4)],
        first_attempt_count: Long.fromInt(1),
        max_retries: Long.fromInt(0),
        legacy_last_err: "",
        num_rows: { mean: 0, squared_diffs: 0 },
        parse_lat: { mean: 0, squared_diffs: 0 },
        plan_lat: { mean: 0.018638, squared_diffs: 0 },
        run_lat: { mean: 0.008555, squared_diffs: 0 },
        service_lat: { mean: -9223372036.82758, squared_diffs: 0 },
        overhead_lat: { mean: -9223372036.854773, squared_diffs: 0 },
        legacy_last_err_redacted: "",
        sensitive_info: {
          last_err: "",
          most_recent_plan_description: {
            name: "scan",
            attrs: [
              { key: "missing stats", value: "" },
              {
                key: "table",
                value: "settings@primary",
              },
              { key: "spans", value: "1 span" },
            ],
          },
          most_recent_plan_timestamp: {
            seconds: Long.fromInt(1599667573),
            nanos: 342990000,
          },
        },
        bytes_read: { mean: 62, squared_diffs: 0 },
        rows_read: { mean: 1, squared_diffs: 0 },
      },
      id: Long.fromInt(106),
    },
    {
      key: {
        key_data: {
          query:
            "WITH current_meta AS (SELECT version, num_records, num_spans, total_bytes FROM system.protected_ts_meta UNION ALL SELECT _ AS version, _ AS num_records, _ AS num_spans, _ AS total_bytes ORDER BY version DESC LIMIT _) SELECT version, num_records, num_spans, total_bytes FROM current_meta",
          app: "$ internal-protectedts-GetMetadata",
          distSQL: false,
          failed: false,
          opt: true,
          implicit_txn: false,
          vec: false,
        },
        node_id: 5,
      },
      stats: {
        count: Long.fromInt(24),
        nodes: [Long.fromNumber(2), Long.fromNumber(3)],
        first_attempt_count: Long.fromInt(24),
        max_retries: Long.fromInt(0),
        legacy_last_err: "",
        num_rows: { mean: 0, squared_diffs: 0 },
        parse_lat: { mean: 0.0010775, squared_diffs: 0.000022485897999999995 },
        plan_lat: {
          mean: 0.029394708333333335,
          squared_diffs: 0.1030786466649583,
        },
        run_lat: {
          mean: 0.005224291666666666,
          squared_diffs: 0.0025294570249583337,
        },
        service_lat: {
          mean: 0.035700833333333334,
          squared_diffs: 0.13060311153333334,
        },
        overhead_lat: {
          mean: 0.000004333333333333095,
          squared_diffs: 1.3933333333305632e-10,
        },
        legacy_last_err_redacted: "",
        sensitive_info: {
          last_err: "",
          most_recent_plan_description: {
            name: "render",
            children: [
              {
                name: "limit",
                attrs: [{ key: "count", value: "_" }],
                children: [
                  {
                    name: "sort",
                    attrs: [{ key: "order", value: "-version" }],
                    children: [
                      {
                        name: "union all",
                        children: [
                          {
                            name: "scan",
                            attrs: [
                              { key: "missing stats", value: "" },
                              {
                                key: "table",
                                value: "protected_ts_meta@primary",
                              },
                              { key: "spans", value: "FULL SCAN" },
                            ],
                          },
                          {
                            name: "values",
                            attrs: [{ key: "size", value: "1 column, 1 row" }],
                          },
                        ],
                      },
                    ],
                  },
                ],
              },
            ],
          },
          most_recent_plan_timestamp: {
            seconds: Long.fromInt(1599670094),
            nanos: 152349000,
          },
        },
        bytes_read: { mean: 0, squared_diffs: 0 },
        rows_read: { mean: 0, squared_diffs: 0 },
      },
      id: Long.fromInt(107),
    },
    {
      key: {
        key_data: {
          query:
            "WITH deleted_sessions AS (DELETE FROM sqlliveness WHERE expiration < $1 RETURNING session_id) SELECT count(*) FROM deleted_sessions",
          app: "$ internal-delete-sessions",
          distSQL: false,
          failed: false,
          opt: true,
          implicit_txn: true,
          vec: false,
        },
        node_id: 5,
      },
      stats: {
        count: Long.fromInt(141),
        nodes: [Long.fromNumber(1), Long.fromNumber(2), Long.fromNumber(3)],
        first_attempt_count: Long.fromInt(141),
        max_retries: Long.fromInt(0),
        legacy_last_err: "",
        num_rows: { mean: 0, squared_diffs: 0 },
        parse_lat: { mean: 0, squared_diffs: 0 },
        plan_lat: {
          mean: 0.003381439716312056,
          squared_diffs: 0.08767511878473759,
        },
        run_lat: {
          mean: 0.018693638297872336,
          squared_diffs: 1.045614239546554,
        },
        service_lat: {
          mean: -9223372036.832695,
          squared_diffs: 1.5448581299024227,
        },
        overhead_lat: {
          mean: -9223372036.854767,
          squared_diffs: 6.837217370048165e-8,
        },
        legacy_last_err_redacted: "",
        sensitive_info: {
          last_err: "",
          most_recent_plan_description: {
            name: "root",
            children: [
              {
                name: "group (scalar)",
                children: [
                  {
                    name: "scan buffer",
                    attrs: [
                      { key: "label", value: "buffer 1 (deleted_sessions)" },
                    ],
                  },
                ],
              },
              {
                name: "subquery",
                attrs: [
                  { key: "id", value: "@S1" },
                  {
                    key: "original sql",
                    value:
                      "DELETE FROM sqlliveness WHERE expiration < $1 RETURNING session_id",
                  },
                  { key: "exec mode", value: "all rows" },
                ],
                children: [
                  {
                    name: "buffer",
                    attrs: [
                      { key: "label", value: "buffer 1 (deleted_sessions)" },
                    ],
                    children: [
                      {
                        name: "delete",
                        attrs: [{ key: "from", value: "sqlliveness" }],
                        children: [
                          {
                            name: "filter",
                            attrs: [{ key: "filter", value: "expiration < _" }],
                            children: [
                              {
                                name: "scan",
                                attrs: [
                                  { key: "missing stats", value: "" },
                                  {
                                    key: "table",
                                    value: "sqlliveness@primary",
                                  },
                                  { key: "spans", value: "FULL SCAN" },
                                ],
                              },
                            ],
                          },
                        ],
                      },
                    ],
                  },
                ],
              },
            ],
          },
          most_recent_plan_timestamp: {
            seconds: Long.fromInt(1599670345),
            nanos: 756723000,
          },
        },
        bytes_read: { mean: 0, squared_diffs: 0 },
        rows_read: { mean: 0, squared_diffs: 0 },
      },
      id: Long.fromInt(108),
    },
    {
      key: {
        key_data: {
          query:
            'INSERT INTO system.eventlog("timestamp", "eventType", "targetID", "reportingID", info) VALUES (now(), $1, $2, $3, $4)',
          app: "$ internal-log-event",
          distSQL: false,
          failed: false,
          opt: true,
          implicit_txn: false,
          vec: false,
        },
        node_id: 5,
      },
      stats: {
        count: Long.fromInt(1),
        nodes: [
          Long.fromNumber(1),
          Long.fromNumber(2),
          Long.fromNumber(3),
          Long.fromNumber(4),
        ],
        first_attempt_count: Long.fromInt(1),
        max_retries: Long.fromInt(0),
        legacy_last_err: "",
        num_rows: { mean: 1, squared_diffs: 0 },
        parse_lat: { mean: 0, squared_diffs: 0 },
        plan_lat: { mean: 0.032205, squared_diffs: 0 },
        run_lat: { mean: 0.001689, squared_diffs: 0 },
        service_lat: { mean: -9223372036.820879, squared_diffs: 0 },
        overhead_lat: { mean: -9223372036.854773, squared_diffs: 0 },
        legacy_last_err_redacted: "",
        sensitive_info: {
          last_err: "",
          most_recent_plan_description: {
            name: "insert",
            attrs: [
              {
                key: "into",
                value:
                  "eventlog(timestamp, eventType, targetID, reportingID, info, uniqueID)",
              },
            ],
            children: [
              {
                name: "values",
                attrs: [{ key: "size", value: "6 columns, 1 row" }],
              },
            ],
          },
          most_recent_plan_timestamp: {
            seconds: Long.fromInt(1599667573),
            nanos: 330423000,
          },
        },
        bytes_read: { mean: 0, squared_diffs: 0 },
        rows_read: { mean: 0, squared_diffs: 0 },
      },
      id: Long.fromInt(109),
    },
  ],
  last_reset: { seconds: Long.fromInt(1599667572), nanos: 688635000 },
  internal_app_name_prefix: "$ internal",
  transactions: [
    {
      stats_data: {
        statement_fingerprint_ids: [Long.fromInt(100)],
        app: "$ internal-select-running/get-claimed-jobs",
        stats: {
          count: Long.fromInt(93),
          max_retries: Long.fromInt(0),
          num_rows: { mean: 0, squared_diffs: 0 },
          service_lat: {
            mean: 0.05745331182795698,
            squared_diffs: 14.213222686585958,
          },
          retry_lat: { mean: 0, squared_diffs: 0 },
          commit_lat: {
            mean: 0.000010258064516129034,
            squared_diffs: 3.1277806451612896e-8,
          },
        },
      },
      node_id: 5,
    },
    {
      stats_data: {
        statement_fingerprint_ids: [Long.fromInt(101)],
        app: "$ internal-stmt-diag-poll",
        stats: {
          count: Long.fromInt(281),
          max_retries: Long.fromInt(0),
          num_rows: { mean: 0, squared_diffs: 0 },
          service_lat: {
            mean: 0.024999483985765122,
            squared_diffs: 4.3998952051121805,
          },
          retry_lat: { mean: 0, squared_diffs: 0 },
          commit_lat: {
            mean: 0.000010387900355871875,
            squared_diffs: 1.8959871886121012e-7,
          },
        },
      },
      node_id: 5,
    },
    {
      stats_data: {
        statement_fingerprint_ids: [Long.fromInt(102)],
        app: "$ internal-get-tables",
        stats: {
          count: Long.fromInt(1),
          max_retries: Long.fromInt(0),
          num_rows: { mean: 0, squared_diffs: 0 },
          service_lat: { mean: 0.081996, squared_diffs: 0 },
          retry_lat: { mean: 0, squared_diffs: 0 },
          commit_lat: { mean: 0.000011, squared_diffs: 0 },
        },
      },
      node_id: 5,
    },
    {
      stats_data: {
        statement_fingerprint_ids: [Long.fromInt(103)],
        app: "$ internal-read orphaned leases",
        stats: {
          count: Long.fromInt(1),
          max_retries: Long.fromInt(0),
          num_rows: { mean: 0, squared_diffs: 0 },
          service_lat: { mean: 0.027685, squared_diffs: 0 },
          retry_lat: { mean: 0, squared_diffs: 0 },
          commit_lat: { mean: 0.000004, squared_diffs: 0 },
        },
      },
      node_id: 5,
    },
    {
      stats_data: {
        statement_fingerprint_ids: [Long.fromInt(104)],
        app: "$ internal-expire-sessions",
        stats: {
          count: Long.fromInt(280),
          max_retries: Long.fromInt(0),
          num_rows: { mean: 0, squared_diffs: 0 },
          service_lat: {
            mean: 0.07074643571428572,
            squared_diffs: 192.57823989665084,
          },
          retry_lat: { mean: 0, squared_diffs: 0 },
          commit_lat: { mean: 0, squared_diffs: 0 },
        },
      },
      node_id: 5,
    },
    {
      stats_data: {
        statement_fingerprint_ids: [Long.fromInt(105)],
        app: "$ internal-show-version",
        stats: {
          count: Long.fromInt(1),
          max_retries: Long.fromInt(0),
          num_rows: { mean: 0, squared_diffs: 0 },
          service_lat: { mean: 0.066901, squared_diffs: 0 },
          retry_lat: { mean: 0, squared_diffs: 0 },
          commit_lat: { mean: 0.000003, squared_diffs: 0 },
        },
      },
      node_id: 5,
    },
    {
      stats_data: {
        statement_fingerprint_ids: [Long.fromInt(106), Long.fromInt(107)],
        app: "$ internal-delete-sessions",
        stats: {
          count: Long.fromInt(141),
          max_retries: Long.fromInt(0),
          num_rows: { mean: 0, squared_diffs: 0 },
          service_lat: {
            mean: 0.04333560992907801,
            squared_diffs: 11.010815031621545,
          },
          retry_lat: { mean: 0, squared_diffs: 0 },
          commit_lat: {
            mean: 0.000010248226950354606,
            squared_diffs: 5.068431205673761e-8,
          },
        },
      },
      node_id: 5,
    },
    {
      stats_data: {
        statement_fingerprint_ids: [Long.fromInt(108)],
        app: "$ TEST",
        stats: {
          count: Long.fromInt(278),
          max_retries: Long.fromInt(0),
          num_rows: { mean: 5, squared_diffs: 0 },
          service_lat: {
            mean: 0.10633510071942444,
            squared_diffs: 354.59311369326707,
          },
          retry_lat: { mean: 0, squared_diffs: 0 },
          commit_lat: { mean: 0, squared_diffs: 0 },
        },
      },
      node_id: 4,
    },
    {
      stats_data: {
        statement_fingerprint_ids: [Long.fromInt(109)],
        app: "$ TEST",
        stats: {
          count: Long.fromInt(140),
          max_retries: Long.fromInt(0),
          num_rows: { mean: 3, squared_diffs: 0 },
          service_lat: {
            mean: 0.08878077142857142,
            squared_diffs: 105.5228685349407,
          },
          retry_lat: { mean: 0, squared_diffs: 0 },
          commit_lat: {
            mean: 0.00000877857142857143,
            squared_diffs: 5.338135714285717e-9,
          },
        },
      },
      node_id: 4,
    },
    {
      stats_data: {
        statement_fingerprint_ids: [Long.fromInt(107)],
        app: "$ TEST",
        stats: {
          count: Long.fromInt(280),
          max_retries: Long.fromInt(0),
          num_rows: { mean: 0, squared_diffs: 0 },
          service_lat: {
            mean: 0.06983223571428572,
            squared_diffs: 146.70526074151044,
          },
          retry_lat: { mean: 0, squared_diffs: 0 },
          commit_lat: {
            mean: 0.000008235714285714293,
            squared_diffs: 1.5384442857142857e-8,
          },
        },
      },
      node_id: 4,
    },
    {
      stats_data: {
        statement_fingerprint_ids: [Long.fromInt(107)],
        app: "$ TEST EXACT",
        stats: {
          count: Long.fromInt(280),
          max_retries: Long.fromInt(0),
          num_rows: { mean: 0, squared_diffs: 0 },
          service_lat: {
            mean: 0.01983223571428572,
            squared_diffs: 146.70526074151044,
          },
          retry_lat: { mean: 0, squared_diffs: 0 },
          commit_lat: {
            mean: 0.000008235714285714293,
            squared_diffs: 1.5384442857142857e-8,
          },
        },
      },
      node_id: 4,
    },
  ],
};
