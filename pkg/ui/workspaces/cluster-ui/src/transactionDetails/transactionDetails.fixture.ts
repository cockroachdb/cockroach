// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import * as protos from "@cockroachlabs/crdb-protobuf-client";
import { createMemoryHistory } from "history";
import Long from "long";
import moment from "moment-timezone";

import { StatementsResponse } from "src/store/sqlStats/sqlStats.reducer";

import { TimeScale } from "../timeScaleDropdown";
import { RequestError } from "../util";

const history = createMemoryHistory({ initialEntries: ["/transactions"] });
const timestamp = new protos.google.protobuf.Timestamp({
  seconds: new Long(Date.parse("Nov 26 2021 01:00:00 GMT") * 1e-3),
});
export const transactionFingerprintId = Long.fromString("3632089240731979669");

export const routeProps = {
  history,
  location: {
    pathname: `/transaction/${transactionFingerprintId}`,
    search: "",
    hash: "",
    state: {},
  },
  match: {
    path: "/transaction/:txn_fingerprint_id",
    url: `/transaction/${transactionFingerprintId}`,
    isExact: true,
    params: {
      txn_fingerprint_id: transactionFingerprintId,
    },
  },
};
export const nodeRegions = {
  "1": "gcp-us-east1",
  "2": "gcp-us-east1",
  "3": "gcp-us-west1",
  "4": "gcp-europe-west1",
};

export const error = new RequestError(
  403,
  "this operation requires admin privilege",
);

export const transaction = {
  stats_data: {
    statement_fingerprint_ids: [
      Long.fromString("4176684928840388768"),
      Long.fromString("18377382163116490400"),
    ],
    app: "$ cockroach sql",
    stats: {
      count: new Long(1),
      max_retries: new Long(0),
      num_rows: {
        mean: 6,
        squared_diffs: 0,
      },
      service_lat: {
        mean: 0.001121754,
        squared_diffs: 0,
      },
      retry_lat: {
        mean: 0,
        squared_diffs: 0,
      },
      commit_lat: {
        mean: 0.000021931,
        squared_diffs: 0,
      },
      bytes_read: {
        mean: 0,
        squared_diffs: 0,
      },
      rows_read: {
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
      rows_written: {
        mean: 0,
        squared_diffs: 0,
      },
    },
    aggregated_ts: timestamp,
    transaction_fingerprint_id: transactionFingerprintId,
    aggregation_interval: {
      seconds: new Long(3600),
    },
  },
};

export const transactionDetailsData: StatementsResponse = {
  stmts_total_runtime_secs: 1,
  txns_total_runtime_secs: 1,
  oldest_aggregated_ts_returned: timestamp,
  stmts_source_table: "crdb_internal.statement_activity",
  txns_source_table: "crdb_internal.transaction_activity",
  statements: [
    {
      key: {
        key_data: {
          query: "SELECT * FROM crdb_internal.node_build_info",
          app: "$ cockroach sql",
          distSQL: false,
          implicit_txn: true,
          vec: true,
          full_scan: false,
          database: "movr",
          plan_hash: new Long(0),
          transaction_fingerprint_id: transactionFingerprintId,
          query_summary: "SELECT * FROM crdb_internal.node_build_info",
        },
        aggregated_ts: timestamp,
        aggregation_interval: {
          seconds: new Long(3600),
        },
      },
      stats: {
        count: new Long(1),
        failure_count: new Long(0),
        first_attempt_count: new Long(1),
        max_retries: new Long(0),
        legacy_last_err: "",
        num_rows: {
          mean: 6,
          squared_diffs: 0,
        },
        parse_lat: {
          mean: 0.00017026,
          squared_diffs: 0,
        },
        plan_lat: {
          mean: 0.000188651,
          squared_diffs: 0,
        },
        run_lat: {
          mean: 0.000255685,
          squared_diffs: 0,
        },
        service_lat: {
          mean: 0.000629367,
          squared_diffs: 0,
        },
        overhead_lat: {
          mean: 0.000014771000000000012,
          squared_diffs: 0,
        },
        legacy_last_err_redacted: "",
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
          },
          most_recent_plan_timestamp: {
            seconds: new Long(-2135596800),
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
          seconds: new Long(1650591005),
          nanos: 677408609,
        },
        nodes: [new Long(2)],
        kv_node_ids: [2],
        rows_written: {
          mean: 0,
          squared_diffs: 0,
        },
        plan_gists: ["AgH6////nxoAAA4AAAAGBg=="],
      },
      id: Long.fromString("4176684928840388768"),
    },
    {
      key: {
        key_data: {
          query: "SET sql_safe_updates = _",
          app: "$ cockroach sql",
          distSQL: false,
          implicit_txn: true,
          vec: true,
          full_scan: false,
          database: "movr",
          plan_hash: new Long(0),
          transaction_fingerprint_id: Long.fromString("5794495518355343743"),
          query_summary: "SET sql_safe_updates = _",
        },
        aggregated_ts: timestamp,
        aggregation_interval: {
          seconds: new Long(3600),
        },
      },
      stats: {
        count: new Long(1),
        failure_count: new Long(0),
        first_attempt_count: new Long(1),
        max_retries: new Long(0),
        legacy_last_err: "",
        num_rows: {
          mean: 0,
          squared_diffs: 0,
        },
        parse_lat: {
          mean: 0.000045175,
          squared_diffs: 0,
        },
        plan_lat: {
          mean: 0.000065382,
          squared_diffs: 0,
        },
        run_lat: {
          mean: 0.000131952,
          squared_diffs: 0,
        },
        service_lat: {
          mean: 0.000250045,
          squared_diffs: 0,
        },
        overhead_lat: {
          mean: 0.00000753599999999999,
          squared_diffs: 0,
        },
        legacy_last_err_redacted: "",
        sensitive_info: {
          last_err: "",
          most_recent_plan_description: {
            name: "set",
          },
          most_recent_plan_timestamp: {
            seconds: new Long(-2135596800),
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
        sql_type: "TypeDCL",
        last_exec_timestamp: {
          seconds: new Long(1650591005),
          nanos: 801046328,
        },
        nodes: [new Long(2)],
        kv_node_ids: [2],
        rows_written: {
          mean: 0,
          squared_diffs: 0,
        },
        plan_gists: ["Ais="],
      },
      id: Long.fromString("18377382163116490400"),
    },
    {
      key: {
        key_data: {
          query: "SELECT * FROM users",
          app: "$ cockroach sql",
          distSQL: false,
          implicit_txn: true,
          vec: true,
          full_scan: true,
          database: "movr",
          plan_hash: new Long(0),
          transaction_fingerprint_id: Long.fromString("13388351560861020642"),
          query_summary: "SELECT * FROM users",
        },
        aggregated_ts: timestamp,
        aggregation_interval: {
          seconds: new Long(3600),
        },
      },
      stats: {
        count: new Long(2),
        failure_count: new Long(0),
        first_attempt_count: new Long(2),
        max_retries: new Long(0),
        legacy_last_err: "",
        num_rows: {
          mean: 8,
          squared_diffs: 0,
        },
        parse_lat: {
          mean: 0.000066329,
          squared_diffs: 6.509404999999994e-11,
        },
        plan_lat: {
          mean: 0.0004147245,
          squared_diffs: 7.797132564500002e-9,
        },
        run_lat: {
          mean: 0.0036602285,
          squared_diffs: 0.000005232790426512499,
        },
        service_lat: {
          mean: 0.0041592605,
          squared_diffs: 0.0000056896068047405,
        },
        overhead_lat: {
          mean: 0.00001797850000000048,
          squared_diffs: 1.9345445000014194e-12,
        },
        legacy_last_err_redacted: "",
        sensitive_info: {
          last_err: "",
          most_recent_plan_description: {
            name: "scan",
            attrs: [
              {
                key: "Estimated Row Count",
                value: "8 (100% of the table; stats collected 20 days ago)",
              },
              {
                key: "Spans",
                value: "FULL SCAN",
              },
              {
                key: "Table",
                value: "users@users_pkey",
              },
            ],
          },
          most_recent_plan_timestamp: {
            seconds: new Long(-2135596800),
          },
        },
        bytes_read: {
          mean: 918,
          squared_diffs: 0,
        },
        rows_read: {
          mean: 8,
          squared_diffs: 0,
        },
        exec_stats: {
          count: new Long(2),
          network_bytes: {
            mean: 0,
            squared_diffs: 0,
          },
          max_mem_usage: {
            mean: 20480,
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
          seconds: new Long(1650591014),
          nanos: 178832951,
        },
        nodes: [new Long(2)],
        kv_node_ids: [2],
        rows_written: {
          mean: 0,
          squared_diffs: 0,
        },
        plan_gists: ["AgHUAQIAHwAAAAYK"],
      },
      id: Long.fromString("1634603821603440189"),
    },
  ],
  last_reset: {
    seconds: new Long(1651000310),
    nanos: 419008978,
  },
  internal_app_name_prefix: "$ internal",
  transactions: [
    transaction,
    {
      stats_data: {
        statement_fingerprint_ids: [Long.fromString("1634603821603440189")],
        app: "$ cockroach sql",
        stats: {
          count: new Long(2),
          max_retries: new Long(0),
          num_rows: {
            mean: 8,
            squared_diffs: 0,
          },
          service_lat: {
            mean: 0.0047974385,
            squared_diffs: 0.0000059061373681005006,
          },
          retry_lat: {
            mean: 0,
            squared_diffs: 0,
          },
          commit_lat: {
            mean: 0.000031955499999999996,
            squared_diffs: 1.4620500000000385e-14,
          },
          bytes_read: {
            mean: 918,
            squared_diffs: 0,
          },
          rows_read: {
            mean: 8,
            squared_diffs: 0,
          },
          exec_stats: {
            count: new Long(2),
            network_bytes: {
              mean: 0,
              squared_diffs: 0,
            },
            max_mem_usage: {
              mean: 20480,
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
          rows_written: {
            mean: 0,
            squared_diffs: 0,
          },
        },
        aggregated_ts: timestamp,
        transaction_fingerprint_id: Long.fromString("13388351560861020642"),
        aggregation_interval: {
          seconds: new Long(3600),
        },
      },
    },
  ],
};

export const timeScale: TimeScale = {
  windowSize: moment.duration(1, "year"),
  sampleSize: moment.duration(1, "day"),
  fixedWindowEnd: moment.utc("2021.12.31"),
  key: "Custom",
};

export const requestTime = moment.utc("2023.01.5");
