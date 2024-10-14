// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import Long from "long";

import { uniqueLong, unique } from "src/util/arrays";
import { TimestampToNumber, DurationToNumber } from "src/util/convert";
import { FixLong } from "src/util/fixLong";

export type StatementStatistics = cockroach.sql.IStatementStatistics;
export type ExecStats = cockroach.sql.IExecStats;
export type CollectedStatementStatistics =
  cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;

export interface NumericStat {
  mean?: number;
  squared_diffs?: number;
}

export function variance(stat: NumericStat, count: number): number {
  return (stat?.squared_diffs || 0) / (count - 1);
}

export function stdDev(stat: NumericStat, count: number): number {
  return Math.sqrt(variance(stat, count)) || 0;
}

export function stdDevLong(stat: NumericStat, count: number | Long): number {
  return stdDev(stat, Number(FixLong(count)));
}

// aggregateNumericStats computes a new `NumericStat` instance from 2 arguments by using the counts
// to generate new `mean` and `squared_diffs` values.
export function aggregateNumericStats(
  a: NumericStat,
  b: NumericStat,
  countA: number,
  countB: number,
): { mean: number; squared_diffs: number } {
  const total = countA + countB;
  const delta = b?.mean - a?.mean;

  return {
    mean: (a?.mean * countA + b?.mean * countB) / total,
    squared_diffs:
      a?.squared_diffs +
      b?.squared_diffs +
      (delta * delta * countA * countB) / total,
  };
}

export function aggregateLatencyInfo(
  a: StatementStatistics,
  b: StatementStatistics,
): cockroach.sql.ILatencyInfo {
  const min =
    a.latency_info?.min === 0 || a.latency_info?.min > b.latency_info?.min
      ? b.latency_info?.min
      : a.latency_info?.min;
  const max =
    a.latency_info?.max > b.latency_info?.max
      ? a.latency_info?.max
      : b.latency_info?.max;

  let p50 = b.latency_info?.p50;
  let p90 = b.latency_info?.p90;
  let p99 = b.latency_info?.p99;
  // Use the latest value we have that is not zero.
  if (
    b.last_exec_timestamp < a.last_exec_timestamp &&
    b.latency_info?.p50 !== 0
  ) {
    p50 = a.latency_info?.p50;
    p90 = a.latency_info?.p90;
    p99 = a.latency_info?.p99;
  }

  return {
    min,
    max,
    p50,
    p90,
    p99,
  };
}

export function coalesceSensitiveInfo(
  a: cockroach.sql.ISensitiveInfo,
  b: cockroach.sql.ISensitiveInfo,
): cockroach.sql.ISensitiveInfo {
  return {
    last_err: a.last_err || b.last_err,
    most_recent_plan_description:
      a.most_recent_plan_description || b.most_recent_plan_description,
  };
}

export function addMaybeUnsetNumericStat(
  a: NumericStat,
  b: NumericStat,
  countA: number,
  countB: number,
): NumericStat {
  return a && b ? aggregateNumericStats(a, b, countA, countB) : null;
}

export function addExecStats(a: ExecStats, b: ExecStats): ExecStats {
  let countA = FixLong(a.count).toInt();
  const countB = FixLong(b.count).toInt();
  if (countA === 0 && countB === 0) {
    // If both counts are zero, artificially set the one count to one to avoid
    // division by zero when calculating the mean in addNumericStats.
    countA = 1;
  }
  return {
    count: a.count.add(b.count),
    network_bytes: addMaybeUnsetNumericStat(
      a.network_bytes,
      b.network_bytes,
      countA,
      countB,
    ),
    max_mem_usage: addMaybeUnsetNumericStat(
      a.max_mem_usage,
      b.max_mem_usage,
      countA,
      countB,
    ),
    contention_time: addMaybeUnsetNumericStat(
      a.contention_time,
      b.contention_time,
      countA,
      countB,
    ),
    network_messages: addMaybeUnsetNumericStat(
      a.network_messages,
      b.network_messages,
      countA,
      countB,
    ),
    max_disk_usage: addMaybeUnsetNumericStat(
      a.max_disk_usage,
      b.max_disk_usage,
      countA,
      countB,
    ),
    cpu_sql_nanos: addMaybeUnsetNumericStat(
      a.cpu_sql_nanos,
      b.cpu_sql_nanos,
      countA,
      countB,
    ),
  };
}

export function addStatementStats(
  a: StatementStatistics,
  b: StatementStatistics,
): Required<StatementStatistics> {
  const countA = FixLong(a.count).toInt();
  const countB = FixLong(b.count).toInt();

  let regions: string[] = [];
  if (a.regions && b.regions) {
    regions = unique(a.regions.concat(b.regions));
  } else if (a.regions) {
    regions = a.regions;
  } else if (b.regions) {
    regions = b.regions;
  }

  let planGists: string[] = [];
  if (a.plan_gists && b.plan_gists) {
    planGists = unique(a.plan_gists.concat(b.plan_gists));
  } else if (a.plan_gists) {
    planGists = a.plan_gists;
  } else if (b.plan_gists) {
    planGists = b.plan_gists;
  }

  let indexRec: string[] = [];
  if (a.index_recommendations && b.index_recommendations) {
    indexRec = unique(a.index_recommendations.concat(b.index_recommendations));
  } else if (a.index_recommendations) {
    indexRec = a.index_recommendations;
  } else if (b.index_recommendations) {
    indexRec = b.index_recommendations;
  }

  let indexes: string[] = [];
  if (a.indexes && b.indexes) {
    indexes = unique(a.indexes.concat(b.indexes));
  } else if (a.indexes) {
    indexes = a.indexes;
  } else if (b.indexes) {
    indexes = b.indexes;
  }

  return {
    count: a.count.add(b.count),
    failure_count: a.failure_count.add(b.failure_count),
    first_attempt_count: a.first_attempt_count.add(b.first_attempt_count),
    max_retries: a.max_retries.greaterThan(b.max_retries)
      ? a.max_retries
      : b.max_retries,
    num_rows: aggregateNumericStats(a.num_rows, b.num_rows, countA, countB),
    idle_lat: aggregateNumericStats(a.idle_lat, b.idle_lat, countA, countB),
    parse_lat: aggregateNumericStats(a.parse_lat, b.parse_lat, countA, countB),
    plan_lat: aggregateNumericStats(a.plan_lat, b.plan_lat, countA, countB),
    run_lat: aggregateNumericStats(a.run_lat, b.run_lat, countA, countB),
    service_lat: aggregateNumericStats(
      a.service_lat,
      b.service_lat,
      countA,
      countB,
    ),
    overhead_lat: aggregateNumericStats(
      a.overhead_lat,
      b.overhead_lat,
      countA,
      countB,
    ),
    bytes_read: aggregateNumericStats(
      a.bytes_read,
      b.bytes_read,
      countA,
      countB,
    ),
    rows_read: aggregateNumericStats(a.rows_read, b.rows_read, countA, countB),
    rows_written: aggregateNumericStats(
      a.rows_written,
      b.rows_written,
      countA,
      countB,
    ),
    sensitive_info: coalesceSensitiveInfo(a.sensitive_info, b.sensitive_info),
    legacy_last_err: "",
    legacy_last_err_redacted: "",
    exec_stats: addExecStats(a.exec_stats, b.exec_stats),
    sql_type: a.sql_type,
    last_exec_timestamp:
      a.last_exec_timestamp &&
      b.last_exec_timestamp &&
      a.last_exec_timestamp.seconds > b.last_exec_timestamp.seconds
        ? a.last_exec_timestamp
        : b.last_exec_timestamp,
    nodes: uniqueLong([...a.nodes, ...b.nodes]),
    kv_node_ids: unique([...a.kv_node_ids, ...b.kv_node_ids]),
    regions: regions,
    used_follower_read: a.used_follower_read || b.used_follower_read,
    plan_gists: planGists,
    index_recommendations: indexRec,
    indexes: indexes,
    latency_info: aggregateLatencyInfo(a, b),
    last_error_code: "",
  };
}

export interface ExecutionStatistics {
  statement_fingerprint_id: Long;
  statement: string;
  statement_summary: string;
  aggregated_ts: number;
  aggregation_interval: number;
  app: string;
  database: string;
  distSQL: boolean;
  vec: boolean;
  implicit_txn: boolean;
  full_scan: boolean;
  node_id: number;
  txn_fingerprint_ids: Long[];
  stats: StatementStatistics;
}

export function flattenStatementStats(
  statementStats: CollectedStatementStatistics[],
): ExecutionStatistics[] {
  return statementStats.map(stmt => ({
    statement_fingerprint_id: stmt.id,
    statement: stmt.key.key_data.query,
    statement_summary: stmt.key.key_data.query_summary,
    aggregated_ts: TimestampToNumber(stmt.key.aggregated_ts),
    aggregation_interval: DurationToNumber(stmt.key.aggregation_interval),
    app: stmt.key.key_data.app,
    database: stmt.key.key_data.database,
    distSQL: stmt.key.key_data.distSQL,
    vec: stmt.key.key_data.vec,
    implicit_txn: stmt.key.key_data.implicit_txn,
    full_scan: stmt.key.key_data.full_scan,
    node_id: stmt.key.node_id,
    txn_fingerprint_ids: stmt.txn_fingerprint_ids,
    stats: stmt.stats,
  }));
}

// This function returns a key based on all parameters
// that should be used to group statements.
// Currently, using only statement_fingerprint_id
// (created by ConstructStatementFingerprintID using:
// query, implicit_txn, database, failed).
export function statementKey(stmt: ExecutionStatistics): string {
  return stmt.statement_fingerprint_id?.toString();
}

// transactionScopedStatementKey is similar to statementKey, except that
// it appends the transactionFingerprintID to the string key it generated.
export function transactionScopedStatementKey(
  stmt: ExecutionStatistics,
): string {
  return statementKey(stmt) + stmt.txn_fingerprint_ids?.toString() + stmt.app;
}

export const generateStmtDetailsToID = (
  fingerprintID: string,
  appNames: string,
  start: Long,
  end: Long,
): string => {
  if (
    appNames &&
    (appNames.includes("$ internal") || appNames.includes("unset"))
  ) {
    const apps = appNames.split(",");
    for (let i = 0; i < apps.length; i++) {
      if (apps[i].includes("$ internal")) {
        apps[i] = "$ internal";
      }
      if (apps[i].includes("unset")) {
        apps[i] = "";
      }
    }
    appNames = unique(apps).sort().toString();
  }
  let generatedID = fingerprintID;
  if (appNames) {
    generatedID += `/${appNames}`;
  }
  if (start) {
    generatedID += `/${start}`;
  }
  if (end) {
    generatedID += `/${end}`;
  }
  return generatedID;
};

export const generateTableID = (db: string, table: string): string => {
  return `${encodeURIComponent(db)}/${encodeURIComponent(table)}`;
};
