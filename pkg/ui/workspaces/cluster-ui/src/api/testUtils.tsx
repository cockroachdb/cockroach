// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach, google } from "@cockroachlabs/crdb-protobuf-client";
import Long from "long";

export type Stmt =
  cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;
export type Txn =
  cockroach.server.serverpb.StatementsResponse.IExtendedCollectedTransactionStatistics;
type ILatencyInfo = cockroach.sql.ILatencyInfo;

const latencyInfo: Required<ILatencyInfo> = {
  min: 0.00008,
  max: 0.00028,
  p50: 0.00015,
  p90: 0.00016,
  p99: 0.00018,
};

const baseStmt: Partial<Stmt> = {
  id: Long.fromString("11871906682067483964"),
  txn_fingerprint_ids: [Long.fromInt(1)],
  key: {
    key_data: {
      query: "SELECT node_id FROM system.statement_statistics",
      app: "$ cockroach sql",
      distSQL: true,
      implicit_txn: true,
      vec: true,
      full_scan: true,
      database: "defaultdb",
      query_summary: "SELECT node_id FROM system.statement_statistics",
      transaction_fingerprint_id: Long.fromInt(1),
    },
    node_id: 0,
  },
  stats: {
    count: Long.fromInt(1),
    failure_count: Long.fromInt(0),
    first_attempt_count: Long.fromInt(1),
    max_retries: Long.fromInt(0),
    num_rows: {
      mean: 1576,
      squared_diffs: 0,
    },
    parse_lat: {
      mean: 0.000044584,
      squared_diffs: 0,
    },
    plan_lat: {
      mean: 0.037206708,
      squared_diffs: 0,
    },
    run_lat: {
      mean: 0.003240459,
      squared_diffs: 0,
    },
    service_lat: {
      mean: 0.040506917,
      squared_diffs: 0,
    },
    overhead_lat: {
      mean: 0.000015166000000003954,
      squared_diffs: 0,
    },
    sensitive_info: {
      last_err: "",
      most_recent_plan_description: {
        name: "",
        attrs: [],
        children: [],
      },
      most_recent_plan_timestamp: new google.protobuf.Timestamp(),
    },
    bytes_read: {
      mean: 162109,
      squared_diffs: 0,
    },
    rows_read: {
      mean: 1576,
      squared_diffs: 0,
    },
    rows_written: {
      mean: 0,
      squared_diffs: 0,
    },
    latency_info: latencyInfo,
    exec_stats: {
      count: Long.fromInt(1),
      network_bytes: {
        mean: 0,
        squared_diffs: 0,
      },
      max_mem_usage: {
        mean: 184320,
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
    last_exec_timestamp: new google.protobuf.Timestamp(),
    plan_gists: ["AgFUBAAgAAAABgI="],
    indexes: ["123@456"],
    index_recommendations: [],
    regions: ["gcp-us-east1", "gcp-us-west1"],
    nodes: [Long.fromNumber(1)],
  },
};

const baseTxn: Partial<Txn> = {
  stats_data: {
    statement_fingerprint_ids: [Long.fromString("18262870370352730905")],
    app: "$ cockroach sql",
    stats: {
      count: Long.fromInt(8),
      max_retries: Long.fromInt(0),
      num_rows: {
        mean: 0,
        squared_diffs: 0,
      },
      service_lat: {
        mean: 0.00013457312500000002,
        squared_diffs: 5.992246806875002e-9,
      },
      retry_lat: {
        mean: 0,
        squared_diffs: 0,
      },
      commit_lat: {
        mean: 0.0000031143749999999997,
        squared_diffs: 1.1728737874999997e-11,
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
        count: Long.fromInt(8),
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
    aggregated_ts: new google.protobuf.Timestamp(),
    transaction_fingerprint_id: Long.fromString("5913510653911377094"),
  },
  node_id: 0,
};

const assignObjectPropsIfExists = <T extends { [key: string]: unknown }>(
  baseObj: T,
  overrides: Partial<T>,
): T => {
  const copiedObj: T = { ...baseObj };

  for (const prop in baseObj) {
    if (overrides[prop] === undefined) {
      continue;
    }

    const val = copiedObj[prop];
    if (
      typeof val === "object" &&
      !Array.isArray(val) &&
      overrides[prop] != null
    ) {
      copiedObj[prop] = assignObjectPropsIfExists(
        val as Record<string, unknown>,
        overrides[prop] as Record<string, unknown>,
      ) as typeof val;
    } else {
      copiedObj[prop] = overrides[prop];
    }
  }

  return copiedObj;
};

export const mockStmtStats = (partialStmt: Partial<Stmt> = {}): Stmt => {
  return assignObjectPropsIfExists(baseStmt, partialStmt);
};

export const mockTxnStats = (partialTxn: Partial<Txn> = {}): Txn => {
  return assignObjectPropsIfExists(baseTxn, partialTxn);
};
