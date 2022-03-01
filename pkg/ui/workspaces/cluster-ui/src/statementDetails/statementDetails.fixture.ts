// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import moment from "moment";
import Long from "long";
import { createMemoryHistory } from "history";
import { noop } from "lodash";
import { StatementDetailsProps } from "./statementDetails";

const history = createMemoryHistory({ initialEntries: ["/statements"] });

export const getStatementDetailsPropsFixture = (): StatementDetailsProps => ({
  history,
  location: {
    pathname: "/statement/true/4705782015019656142",
    search: "",
    hash: "",
    state: null,
  },
  match: {
    path: "/statement/:implicitTxn/:statement",
    url: "/statement/true/4705782015019656142",
    isExact: true,
    params: {
      implicitTxn: "true",
      statement: "4705782015019656142",
    },
  },
  timeScale: {
    windowSize: moment.duration(5, "day"),
    sampleSize: moment.duration(5, "minutes"),
    fixedWindowEnd: moment.utc("2021.12.12"),
    key: "Custom",
  },
  statementDetails: {
    statement: {
      key_data: {
        query: "SELECT * FROM crdb_internal.node_build_info",
        app: "",
        distSQL: false,
        failed: false,
        implicit_txn: true,
        vec: true,
        full_scan: false,
        database: "defaultdb",
        plan_hash: new Long(0),
        query_summary: "SELECT * FROM crdb_internal.node_build_info",
        transaction_fingerprint_id: new Long(0),
      },
      app_names: ["$ cockroach sql", "newname"],
      formatted_query: "SELECT * FROM crdb_internal.node_build_info\n",
      stats: {
        count: new Long(5),
        first_attempt_count: new Long(5),
        max_retries: new Long(0),
        legacy_last_err: "",
        legacy_last_err_redacted: "",
        num_rows: {
          mean: 6,
          squared_diffs: 0,
        },
        parse_lat: {
          mean: 0.0000876,
          squared_diffs: 2.35792e-8,
        },
        plan_lat: {
          mean: 0.008131,
          squared_diffs: 0.00127640837,
        },
        run_lat: {
          mean: 0.0002796,
          squared_diffs: 2.401919999999999e-8,
        },
        service_lat: {
          mean: 0.008522,
          squared_diffs: 0.001298238058,
        },
        overhead_lat: {
          mean: 0.000023799999999999972,
          squared_diffs: 5.492799999999973e-9,
        },
        sensitive_info: {
          last_err: "",
          most_recent_plan_description: {
            name: "virtual table",
            attrs: [
              {
                key: "Table",
                value: "node_build_info@primary",
              },
            ],
            children: [],
          },
          most_recent_plan_timestamp: {
            seconds: new Long(1614851546),
            nanos: 956814000,
          },
        },
        bytes_read: {
          mean: 0,
          squared_diffs: 0,
        },
        rows_read: {
          mean: 0,
          squared_diffs: 0,
        },
        rows_written: {
          mean: 0,
          squared_diffs: 0,
        },
        exec_stats: {
          count: new Long(5),
          network_bytes: {
            mean: 0,
            squared_diffs: 0,
          },
          max_mem_usage: {
            mean: 10240,
            squared_diffs: 0,
          },
          contention_time: {
            mean: 0,
            squared_diffs: 0,
          },
          network_messages: {
            mean: 0,
            squared_diffs: 0,
          },
          max_disk_usage: {
            mean: 0,
            squared_diffs: 0,
          },
        },
        sql_type: "TypeDML",
        last_exec_timestamp: {
          seconds: Long.fromInt(1599670290),
          nanos: 111613000,
        },
        nodes: [new Long(1)],
        plan_gists: ["AgH6////nxkAAA4AAAAGBg=="],
      },
    },
    statements_per_aggregated_ts: [
      {
        stats: {
          count: new Long(1),
          first_attempt_count: new Long(1),
          max_retries: new Long(0),
          legacy_last_err: "",
          legacy_last_err_redacted: "",
          num_rows: {
            mean: 6,
            squared_diffs: 0,
          },
          parse_lat: {
            mean: 0.00004,
            squared_diffs: 0,
          },
          plan_lat: {
            mean: 0.000105,
            squared_diffs: 0,
          },
          run_lat: {
            mean: 0.000285,
            squared_diffs: 0,
          },
          service_lat: {
            mean: 0.000436,
            squared_diffs: 0,
          },
          overhead_lat: {
            mean: 0.000006000000000000037,
            squared_diffs: 0,
          },
          sensitive_info: {
            last_err: "",
            most_recent_plan_description: {
              name: "virtual table",
              attrs: [
                {
                  key: "Table",
                  value: "node_build_info@primary",
                },
              ],
              children: [],
            },
            most_recent_plan_timestamp: {
              seconds: new Long(1614851546),
              nanos: 956814000,
            },
          },
          bytes_read: {
            mean: 0,
            squared_diffs: 0,
          },
          rows_read: {
            mean: 0,
            squared_diffs: 0,
          },
          rows_written: {
            mean: 0,
            squared_diffs: 0,
          },
          exec_stats: {
            count: new Long(1),
            network_bytes: {
              mean: 0,
              squared_diffs: 0,
            },
            max_mem_usage: {
              mean: 10240,
              squared_diffs: 0,
            },
            contention_time: {
              mean: 0,
              squared_diffs: 0,
            },
            network_messages: {
              mean: 0,
              squared_diffs: 0,
            },
            max_disk_usage: {
              mean: 0,
              squared_diffs: 0,
            },
          },
          sql_type: "TypeDML",
          last_exec_timestamp: {
            seconds: Long.fromInt(1599670292),
            nanos: 111613000,
          },
          nodes: [new Long(1)],
          plan_gists: ["AgH6////nxkAAA4AAAAGBg=="],
        },
        aggregated_ts: {
          seconds: Long.fromInt(1599670292),
          nanos: 111613000,
        },
      },
      {
        stats: {
          count: new Long(2),
          first_attempt_count: new Long(2),
          max_retries: new Long(0),
          legacy_last_err: "",
          legacy_last_err_redacted: "",
          num_rows: {
            mean: 6,
            squared_diffs: 0,
          },
          parse_lat: {
            mean: 0.000071,
            squared_diffs: 4.050000000000001e-9,
          },
          plan_lat: {
            mean: 0.0001525,
            squared_diffs: 3.960499999999999e-9,
          },
          run_lat: {
            mean: 0.0002255,
            squared_diffs: 1.08045e-8,
          },
          service_lat: {
            mean: 0.0004555,
            squared_diffs: 1.0224500000000002e-8,
          },
          overhead_lat: {
            mean: 0.000006499999999999995,
            squared_diffs: 4.499999999999893e-12,
          },
          sensitive_info: {
            last_err: "",
            most_recent_plan_description: {
              name: "virtual table",
              attrs: [
                {
                  key: "Table",
                  value: "node_build_info@primary",
                },
              ],
              children: [],
            },
            most_recent_plan_timestamp: {
              seconds: new Long(1614851546),
              nanos: 956814000,
            },
          },
          bytes_read: {
            mean: 0,
            squared_diffs: 0,
          },
          rows_read: {
            mean: 0,
            squared_diffs: 0,
          },
          rows_written: {
            mean: 0,
            squared_diffs: 0,
          },
          exec_stats: {
            count: new Long(2),
            network_bytes: {
              mean: 0,
              squared_diffs: 0,
            },
            max_mem_usage: {
              mean: 10240,
              squared_diffs: 0,
            },
            contention_time: {
              mean: 0,
              squared_diffs: 0,
            },
            network_messages: {
              mean: 0,
              squared_diffs: 0,
            },
            max_disk_usage: {
              mean: 0,
              squared_diffs: 0,
            },
          },
          sql_type: "TypeDML",
          last_exec_timestamp: {
            seconds: Long.fromInt(1599670292),
            nanos: 111613000,
          },
          nodes: [new Long(1)],
          plan_gists: ["AgH6////nxkAAA4AAAAGBg=="],
        },
        aggregated_ts: {
          seconds: Long.fromInt(1599670292),
          nanos: 111613000,
        },
      },
      {
        stats: {
          count: new Long(1),
          first_attempt_count: new Long(1),
          max_retries: new Long(0),
          legacy_last_err: "",
          legacy_last_err_redacted: "",
          num_rows: {
            mean: 6,
            squared_diffs: 0,
          },
          parse_lat: {
            mean: 0.000046,
            squared_diffs: 0,
          },
          plan_lat: {
            mean: 0.000159,
            squared_diffs: 0,
          },
          run_lat: {
            mean: 0.000299,
            squared_diffs: 0,
          },
          service_lat: {
            mean: 0.000514,
            squared_diffs: 0,
          },
          overhead_lat: {
            mean: 0.000010000000000000026,
            squared_diffs: 0,
          },
          sensitive_info: {
            last_err: "",
            most_recent_plan_description: {
              name: "virtual table",
              attrs: [
                {
                  key: "Table",
                  value: "node_build_info@primary",
                },
              ],
              children: [],
            },
            most_recent_plan_timestamp: {
              seconds: new Long(1614851546),
              nanos: 956814000,
            },
          },
          bytes_read: {
            mean: 0,
            squared_diffs: 0,
          },
          rows_read: {
            mean: 0,
            squared_diffs: 0,
          },
          rows_written: {
            mean: 0,
            squared_diffs: 0,
          },
          exec_stats: {
            count: new Long(1),
            network_bytes: {
              mean: 0,
              squared_diffs: 0,
            },
            max_mem_usage: {
              mean: 10240,
              squared_diffs: 0,
            },
            contention_time: {
              mean: 0,
              squared_diffs: 0,
            },
            network_messages: {
              mean: 0,
              squared_diffs: 0,
            },
            max_disk_usage: {
              mean: 0,
              squared_diffs: 0,
            },
          },
          sql_type: "TypeDML",
          last_exec_timestamp: {
            seconds: Long.fromInt(1599670292),
            nanos: 111613000,
          },
          nodes: [new Long(1)],
          plan_gists: ["AgH6////nxkAAA4AAAAGBg=="],
        },
        aggregated_ts: {
          seconds: Long.fromInt(1599671292),
          nanos: 111613000,
        },
      },
      {
        stats: {
          count: new Long(1),
          first_attempt_count: new Long(1),
          max_retries: new Long(0),
          legacy_last_err: "",
          legacy_last_err_redacted: "",
          num_rows: {
            mean: 6,
            squared_diffs: 0,
          },
          parse_lat: {
            mean: 0.00021,
            squared_diffs: 0,
          },
          plan_lat: {
            mean: 0.040086,
            squared_diffs: 0,
          },
          run_lat: {
            mean: 0.000363,
            squared_diffs: 0,
          },
          service_lat: {
            mean: 0.040749,
            squared_diffs: 0,
          },
          overhead_lat: {
            mean: 0.0000899999999999998,
            squared_diffs: 0,
          },
          sensitive_info: {
            last_err: "",
            most_recent_plan_description: {
              name: "virtual table",
              attrs: [
                {
                  key: "Table",
                  value: "node_build_info@primary",
                },
              ],
              children: [],
            },
            most_recent_plan_timestamp: {
              seconds: new Long(1614851546),
              nanos: 956814000,
            },
          },
          bytes_read: {
            mean: 0,
            squared_diffs: 0,
          },
          rows_read: {
            mean: 0,
            squared_diffs: 0,
          },
          rows_written: {
            mean: 0,
            squared_diffs: 0,
          },
          exec_stats: {
            count: new Long(1),
            network_bytes: {
              mean: 0,
              squared_diffs: 0,
            },
            max_mem_usage: {
              mean: 10240,
              squared_diffs: 0,
            },
            contention_time: {
              mean: 0,
              squared_diffs: 0,
            },
            network_messages: {
              mean: 0,
              squared_diffs: 0,
            },
            max_disk_usage: {
              mean: 0,
              squared_diffs: 0,
            },
          },
          sql_type: "TypeDML",
          last_exec_timestamp: {
            seconds: Long.fromInt(1599670292),
            nanos: 111613000,
          },
          nodes: [new Long(1)],
          plan_gists: ["AgH6////nxkAAA4AAAAGBg=="],
        },
        aggregated_ts: {
          seconds: Long.fromInt(1599680292),
          nanos: 111613000,
        },
      },
    ],
    statements_per_plan_hash: [
      {
        stats: {
          count: new Long(5),
          first_attempt_count: new Long(5),
          max_retries: new Long(0),
          legacy_last_err: "",
          legacy_last_err_redacted: "",
          num_rows: {
            mean: 6,
            squared_diffs: 0,
          },
          parse_lat: {
            mean: 0.0000876,
            squared_diffs: 2.35792e-8,
          },
          plan_lat: {
            mean: 0.008131,
            squared_diffs: 0.00127640837,
          },
          run_lat: {
            mean: 0.0002796,
            squared_diffs: 2.401919999999999e-8,
          },
          service_lat: {
            mean: 0.008522,
            squared_diffs: 0.001298238058,
          },
          overhead_lat: {
            mean: 0.000023799999999999972,
            squared_diffs: 5.492799999999973e-9,
          },
          sensitive_info: {
            last_err: "",
            most_recent_plan_description: {
              name: "virtual table",
              attrs: [
                {
                  key: "Table",
                  value: "node_build_info@primary",
                },
              ],
              children: [],
            },
            most_recent_plan_timestamp: {
              seconds: new Long(1614851546),
              nanos: 956814000,
            },
          },
          bytes_read: {
            mean: 0,
            squared_diffs: 0,
          },
          rows_read: {
            mean: 0,
            squared_diffs: 0,
          },
          rows_written: {
            mean: 0,
            squared_diffs: 0,
          },
          exec_stats: {
            count: new Long(5),
            network_bytes: {
              mean: 0,
              squared_diffs: 0,
            },
            max_mem_usage: {
              mean: 10240,
              squared_diffs: 0,
            },
            contention_time: {
              mean: 0,
              squared_diffs: 0,
            },
            network_messages: {
              mean: 0,
              squared_diffs: 0,
            },
            max_disk_usage: {
              mean: 0,
              squared_diffs: 0,
            },
          },
          sql_type: "TypeDML",
          last_exec_timestamp: {
            seconds: Long.fromInt(1599670292),
            nanos: 111613000,
          },
          nodes: [new Long(1)],
          plan_gists: ["AgH6////nxkAAA4AAAAGBg=="],
        },
        explain_plan: "• virtual table\n  table: @primary",
        plan_hash: new Long(14192395335876201826),
      },
    ],
    internal_app_name_prefix: "$ internal",
    toJSON: () => ({}),
  },
  statementsError: null,
  nodeNames: {
    "1": "127.0.0.1:55529 (n1)",
    "2": "127.0.0.1:55532 (n2)",
    "3": "127.0.0.1:55538 (n3)",
    "4": "127.0.0.1:55546 (n4)",
  },
  nodeRegions: {
    "1": "gcp-us-east1",
    "2": "gcp-us-east1",
    "3": "gcp-us-west1",
    "4": "gcp-europe-west1",
  },
  refreshStatementDetails: noop,
  refreshStatementDiagnosticsRequests: noop,
  refreshNodes: noop,
  refreshNodesLiveness: noop,
  refreshUserSQLRoles: noop,
  diagnosticsReports: [],
  dismissStatementDiagnosticsAlertMessage: noop,
  createStatementDiagnosticsReport: noop,
  uiConfig: {
    showStatementDiagnosticsLink: true,
  },
  isTenant: false,
  hasViewActivityRedactedRole: false,
});
