// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { RequestError, TimestampToString } from "../util";
import moment from "moment";
import { createMemoryHistory } from "history";
import Long from "long";
import * as protos from "@cockroachlabs/crdb-protobuf-client";
import { TimeScale } from "../timeScaleDropdown";

const history = createMemoryHistory({ initialEntries: ["/transactions"] });
const timestamp = new protos.google.protobuf.Timestamp({
  seconds: new Long(Date.parse("Nov 26 2021 01:00:00 GMT") * 1e-3),
});
const timestampString = TimestampToString(timestamp);

export const routeProps = {
  history,
  location: {
    pathname: `/transaction/${timestampString}/3632089240731979669`,
    search: "",
    hash: "",
    state: {},
  },
  match: {
    path: "/transaction/:aggregated_ts/:txn_fingerprint_id",
    url: `/transaction/${timestampString}/3632089240731979669`,
    isExact: true,
    params: {
      aggregated_ts: timestampString,
      txn_fingerprint_id: "3632089240731979669",
    },
  },
};

export const transactionDetails = {
  data: {
    statements: [
      {
        label:
          "WITH deleted_sessions AS (DELETE FROM sqlliveness WHERE expiration < $1 RETURNING session_id) SELECT count(*) FROM deleted_sessions",
        key: {
          key_data: {
            id: "673bf9d0055bbae332ad497072db9bbf",
            query:
              "WITH deleted_sessions AS (DELETE FROM sqlliveness WHERE expiration < $1 RETURNING session_id) SELECT count(*) FROM deleted_sessions",
            app: "$ internal-delete-sessions",
            distSQL: false,
            failed: false,
            opt: true,
            implicit_txn: true,
            vec: false,
          },
          node_id: 1,
        },
        stats: {
          count: "164",
          first_attempt_count: "164",
          max_retries: "0",
          legacy_last_err: "",
          num_rows: { mean: 0, squared_diffs: 0 },
          parse_lat: { mean: 0, squared_diffs: 0 },
          plan_lat: {
            mean: 0.0017302560975609757,
            squared_diffs: 0.027190766097243916,
          },
          run_lat: {
            mean: 0.0010671524390243902,
            squared_diffs: 0.0024573014511890235,
          },
          service_lat: {
            mean: 0.011479207317073171,
            squared_diffs: 1.822788002048951,
          },
          overhead_lat: {
            mean: 0.0086817987804878,
            squared_diffs: 1.38481972086236,
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
                              attrs: [
                                { key: "filter", value: "expiration < _" },
                              ],
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
              seconds: "1598943488",
              nanos: 956814000,
            },
          },
          bytes_read: { mean: 0, squared_diffs: 0 },
          rows_read: { mean: 0, squared_diffs: 0 },
          rows_written: { mean: 1, squared_diffs: 0.99 },
        },
      },
      {
        label: 'SELECT "generated" FROM system.reports_meta WHERE id = $1',
        key: {
          key_data: {
            id: "60d2c10043884cb136674a449007bc52",
            query: 'SELECT "generated" FROM system.reports_meta WHERE id = $1',
            app: "$ internal-get-previous-timestamp",
            distSQL: false,
            failed: false,
            opt: true,
            implicit_txn: false,
            vec: false,
          },
          node_id: 1,
        },
        stats: {
          count: "165",
          first_attempt_count: "165",
          max_retries: "0",
          legacy_last_err: "",
          num_rows: { mean: 0, squared_diffs: 0 },
          parse_lat: { mean: 0, squared_diffs: 0 },
          plan_lat: {
            mean: 0.0010962727272727276,
            squared_diffs: 0.0002159174727272727,
          },
          run_lat: {
            mean: 0.00037714545454545445,
            squared_diffs: 0.00006664950650909095,
          },
          service_lat: {
            mean: 0.00308030303030303,
            squared_diffs: 0.0004142852428484848,
          },
          overhead_lat: {
            mean: 0.0016068848484848484,
            squared_diffs: 0.00004782026481212121,
          },
          legacy_last_err_redacted: "",
          sensitive_info: {
            last_err: "",
            most_recent_plan_description: {
              name: "scan",
              attrs: [
                { key: "missing stats", value: "" },
                { key: "table", value: "reports_meta@primary" },
                { key: "spans", value: "1 span" },
              ],
            },
            most_recent_plan_timestamp: {
              seconds: "1598943469",
              nanos: 391020000,
            },
          },
          bytes_read: {
            mean: 37.87272727272729,
            squared_diffs: 18.32727272727264,
          },
          rows_read: { mean: 1, squared_diffs: 0 },
          rows_written: { mean: 1, squared_diffs: 0.99 },
        },
      },
      {
        label:
          'SELECT id, "parentID", "parentSchemaID", name FROM system.namespace',
        key: {
          key_data: {
            id: "d469b9ad35098627c7bc28a0eca86f0a",
            query:
              'SELECT id, "parentID", "parentSchemaID", name FROM system.namespace',
            app: "$ internal-get-all-names",
            distSQL: false,
            failed: false,
            opt: true,
            implicit_txn: false,
            vec: false,
          },
          node_id: 1,
        },
        stats: {
          count: "29",
          first_attempt_count: "29",
          max_retries: "0",
          legacy_last_err: "",
          num_rows: { mean: 0, squared_diffs: 0 },
          parse_lat: {
            mean: 0.0011247241379310346,
            squared_diffs: 0.0002293455597931035,
          },
          plan_lat: {
            mean: 0.0036076896551724123,
            squared_diffs: 0.001673707710206896,
          },
          run_lat: {
            mean: 0.0005267241379310345,
            squared_diffs: 0.000006591083793103449,
          },
          service_lat: {
            mean: 0.005386103448275863,
            squared_diffs: 0.0018046449686896554,
          },
          overhead_lat: {
            mean: 0.00012696551724137938,
            squared_diffs: 5.500029655172411e-7,
          },
          legacy_last_err_redacted: "",
          sensitive_info: {
            last_err: "",
            most_recent_plan_description: {
              name: "scan",
              attrs: [
                { key: "missing stats", value: "" },
                { key: "table", value: "namespace2@primary" },
                { key: "spans", value: "FULL SCAN" },
              ],
            },
            most_recent_plan_timestamp: {
              seconds: "1598943058",
              nanos: 815110000,
            },
          },
          bytes_read: { mean: 1570, squared_diffs: 0 },
          rows_read: { mean: 35, squared_diffs: 0 },
          rows_written: { mean: 1, squared_diffs: 0.99 },
        },
      },
      {
        label:
          "SELECT (SELECT count(*) FROM system.jobs AS j WHERE ((j.created_by_type = _) AND (j.created_by_id = s.schedule_id)) AND (j.status NOT IN (_, _, __more1__))) AS num_running, s.* FROM system.scheduled_jobs AS s WHERE next_run < current_timestamp() ORDER BY random() LIMIT _",
        key: {
          key_data: {
            query:
              "SELECT (SELECT count(*) FROM system.jobs AS j WHERE ((j.created_by_type = _) AND (j.created_by_id = s.schedule_id)) AND (j.status NOT IN (_, _, __more1__))) AS num_running, s.* FROM system.scheduled_jobs AS s WHERE next_run < current_timestamp() ORDER BY random() LIMIT _",
            app: "$ internal-find-scheduled-jobs",
            distSQL: false,
            failed: false,
            opt: true,
            implicit_txn: false,
            vec: false,
          },
          node_id: 1,
        },
        stats: {
          count: "55",
          first_attempt_count: "55",
          max_retries: "0",
          legacy_last_err: "",
          num_rows: { mean: 0, squared_diffs: 0 },
          parse_lat: { mean: 0.0002004, squared_diffs: 8.563692e-7 },
          plan_lat: {
            mean: 0.0033483454545454546,
            squared_diffs: 0.00012831949443636365,
          },
          run_lat: {
            mean: 0.001046581818181818,
            squared_diffs: 0.00015142185738181813,
          },
          service_lat: {
            mean: 0.004701018181818182,
            squared_diffs: 0.00028161206498181825,
          },
          overhead_lat: {
            mean: 0.0001056909090909091,
            squared_diffs: 1.1588574545454597e-7,
          },
          legacy_last_err_redacted: "",
          sensitive_info: {
            last_err: "",
            most_recent_plan_description: {
              name: "limit",
              attrs: [{ key: "count", value: "_" }],
              children: [
                {
                  name: "sort",
                  attrs: [{ key: "order", value: "+column26" }],
                  children: [
                    {
                      name: "render",
                      children: [
                        {
                          name: "group",
                          attrs: [
                            { key: "group by", value: "schedule_id" },
                            { key: "ordered", value: "+schedule_id" },
                          ],
                          children: [
                            {
                              name: "merge join (left outer)",
                              attrs: [
                                {
                                  key: "equality",
                                  value: "(schedule_id) = (created_by_id)",
                                },
                                { key: "left cols are key", value: "" },
                              ],
                              children: [
                                {
                                  name: "filter",
                                  attrs: [
                                    { key: "filter", value: "next_run < _" },
                                  ],
                                  children: [
                                    {
                                      name: "scan",
                                      attrs: [
                                        { key: "missing stats", value: "" },
                                        {
                                          key: "table",
                                          value: "scheduled_jobs@primary",
                                        },
                                        { key: "spans", value: "FULL SCAN" },
                                      ],
                                    },
                                  ],
                                },
                                {
                                  name: "filter",
                                  attrs: [
                                    { key: "filter", value: "status NOT IN _" },
                                  ],
                                  children: [
                                    {
                                      name: "scan",
                                      attrs: [
                                        { key: "missing stats", value: "" },
                                        {
                                          key: "table",
                                          value:
                                            "jobs@jobs_created_by_type_created_by_id_idx",
                                        },
                                        { key: "spans", value: "1 span" },
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
              ],
            },
            most_recent_plan_timestamp: {
              seconds: "1598943460",
              nanos: 53285000,
            },
          },
          bytes_read: { mean: 0, squared_diffs: 0 },
          rows_read: { mean: 0, squared_diffs: 0 },
          rows_written: { mean: 1, squared_diffs: 0.99 },
        },
      },
      {
        label: "SELECT id, config FROM system.zones",
        key: {
          key_data: {
            id: "ba29a6d105491afdc2671ca4bd28ee5W",
            query: "SELECT id, config FROM system.zones",
            app: "$ internal-read-zone-configs",
            distSQL: false,
            failed: false,
            opt: true,
            implicit_txn: true,
            vec: false,
          },
          node_id: 1,
        },
        stats: {
          count: "1",
          first_attempt_count: "1",
          max_retries: "0",
          legacy_last_err: "",
          num_rows: { mean: 0, squared_diffs: 0 },
          parse_lat: { mean: 0.000056, squared_diffs: 0 },
          plan_lat: { mean: 0.000677, squared_diffs: 0 },
          run_lat: { mean: 0.000161, squared_diffs: 0 },
          service_lat: { mean: 0.001023, squared_diffs: 0 },
          overhead_lat: { mean: 0.0001290000000000001, squared_diffs: 0 },
          legacy_last_err_redacted: "",
          sensitive_info: {
            last_err: "",
            most_recent_plan_description: {
              name: "scan",
              attrs: [
                { key: "missing stats", value: "" },
                { key: "table", value: "zones@primary" },
                { key: "spans", value: "FULL SCAN" },
              ],
            },
            most_recent_plan_timestamp: {
              seconds: "1598938499",
              nanos: 112061000,
            },
          },
          bytes_read: { mean: 327, squared_diffs: 0 },
          rows_read: { mean: 7, squared_diffs: 0 },
          rows_written: { mean: 1, squared_diffs: 0.99 },
        },
      },
      {
        label: "SHOW GRANTS ON TABLE system.role_options",
        key: {
          key_data: {
            query: "SHOW GRANTS ON TABLE system.role_options",
            app: "$ internal-admin-show-grants",
            distSQL: false,
            failed: false,
            opt: true,
            implicit_txn: true,
            vec: false,
          },
          node_id: 1,
        },
        stats: {
          count: "1",
          first_attempt_count: "1",
          max_retries: "0",
          legacy_last_err: "",
          num_rows: { mean: 0, squared_diffs: 0 },
          parse_lat: { mean: 0.000243, squared_diffs: 0 },
          plan_lat: { mean: 0.004487, squared_diffs: 0 },
          run_lat: { mean: 0.094834, squared_diffs: 0 },
          service_lat: { mean: 0.100139, squared_diffs: 0 },
          overhead_lat: { mean: 0.0005750000000000061, squared_diffs: 0 },
          legacy_last_err_redacted: "",
          sensitive_info: {
            last_err: "",
            most_recent_plan_description: {
              name: "sort",
              attrs: [{ key: "order", value: "+grantee,+privilege_type" }],
              children: [
                {
                  name: "filter",
                  attrs: [
                    {
                      key: "filter",
                      value: "(table_catalog, table_schema, table_name) IN _",
                    },
                  ],
                  children: [
                    {
                      name: "virtual table",
                      attrs: [
                        { key: "table", value: "table_privileges@primary" },
                      ],
                    },
                  ],
                },
              ],
            },
            most_recent_plan_timestamp: {
              seconds: "1598943060",
              nanos: 614455000,
            },
          },
          bytes_read: { mean: 0, squared_diffs: 0 },
          rows_read: { mean: 0, squared_diffs: 0 },
          rows_written: { mean: 1, squared_diffs: 0.99 },
        },
      },
      {
        label: "SHOW GRANTS ON TABLE system.statement_bundle_chunks",
        key: {
          key_data: {
            query: "SHOW GRANTS ON TABLE system.statement_bundle_chunks",
            app: "$ internal-admin-show-grants",
            distSQL: false,
            failed: false,
            opt: true,
            implicit_txn: true,
            vec: false,
          },
          node_id: 1,
        },
        stats: {
          count: "1",
          first_attempt_count: "1",
          max_retries: "0",
          legacy_last_err: "",
          num_rows: { mean: 0, squared_diffs: 0 },
          parse_lat: { mean: 0.000029, squared_diffs: 0 },
          plan_lat: { mean: 0.004052, squared_diffs: 0 },
          run_lat: { mean: 0.019415, squared_diffs: 0 },
          service_lat: { mean: 0.023886, squared_diffs: 0 },
          overhead_lat: { mean: 0.000389999999999998, squared_diffs: 0 },
          legacy_last_err_redacted: "",
          sensitive_info: {
            last_err: "",
            most_recent_plan_description: {
              name: "sort",
              attrs: [{ key: "order", value: "+grantee,+privilege_type" }],
              children: [
                {
                  name: "filter",
                  attrs: [
                    {
                      key: "filter",
                      value: "(table_catalog, table_schema, table_name) IN _",
                    },
                  ],
                  children: [
                    {
                      name: "virtual table",
                      attrs: [
                        { key: "table", value: "table_privileges@primary" },
                      ],
                    },
                  ],
                },
              ],
            },
            most_recent_plan_timestamp: {
              seconds: "1598943060",
              nanos: 702937000,
            },
          },
          bytes_read: { mean: 0, squared_diffs: 0 },
          rows_read: { mean: 0, squared_diffs: 0 },
          rows_written: { mean: 1, squared_diffs: 0.99 },
        },
      },
    ],
    aggregatedTs: timestampString,
    transactionFingerprintId: "3632089240731979669",
    transaction: {
      stats_data: {
        statement_fingerprint_ids: [
          Long.fromString("673bf9d0055bbae332ad497072db9bbf"),
        ],
        app: "$ internal-select-running/get-claimed-jobs",
        aggregated_ts: timestamp,
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
      regionNodes: ["gcp-us-east1"],
    },
  },
  error: new RequestError(
    "Forbidden",
    403,
    "this operation requires admin privilege",
  ),
  nodeRegions: {
    "1": "gcp-us-east1",
    "2": "gcp-us-east1",
    "3": "gcp-us-west1",
    "4": "gcp-europe-west1",
  },
};

export const timeScale: TimeScale = {
  windowSize: moment.duration(1, "year"),
  sampleSize: moment.duration(1, "day"),
  fixedWindowEnd: moment.utc("2021.12.31"),
  key: "Custom",
};
