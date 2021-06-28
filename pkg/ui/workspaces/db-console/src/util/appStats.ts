// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This file significantly duplicates the algorithms available in
// pkg/roachpb/app_stats.go, in particular the functions on NumericStats
// to compute variance and add together NumericStats.

import _ from "lodash";
import * as protos from "src/js/protos";
import { FixLong } from "src/util/fixLong";
import { uniqueLong } from "src/util/arrays";

export type ISensitiveInfo = protos.cockroach.sql.ISensitiveInfo;
export type StatementStatistics = protos.cockroach.sql.IStatementStatistics;
export type ExecStats = protos.cockroach.sql.IExecStats;
export type CollectedStatementStatistics = protos.cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;

export interface NumericStat {
  mean?: number;
  squared_diffs?: number;
}

export function variance(stat: NumericStat, count: number) {
  return stat.squared_diffs / (count - 1);
}

export function stdDev(stat: NumericStat, count: number) {
  return Math.sqrt(variance(stat, count)) || 0;
}

export function stdDevLong(stat: NumericStat, count: number | Long) {
  return stdDev(stat, FixLong(count).toInt());
}

export function addNumericStats(
  a: NumericStat,
  b: NumericStat,
  countA: number,
  countB: number,
) {
  const total = countA + countB;
  const delta = b.mean - a.mean;

  return {
    mean: (a.mean * countA + b.mean * countB) / total,
    squared_diffs:
      a.squared_diffs +
      b.squared_diffs +
      (delta * delta * countA * countB) / total,
  };
}

export function addStatementStats(
  a: StatementStatistics,
  b: StatementStatistics,
): Required<StatementStatistics> {
  const countA = FixLong(a.count).toInt();
  const countB = FixLong(b.count).toInt();
  return {
    count: a.count.add(b.count),
    first_attempt_count: a.first_attempt_count.add(b.first_attempt_count),
    max_retries: a.max_retries.greaterThan(b.max_retries)
      ? a.max_retries
      : b.max_retries,
    num_rows: addNumericStats(a.num_rows, b.num_rows, countA, countB),
    parse_lat: addNumericStats(a.parse_lat, b.parse_lat, countA, countB),
    plan_lat: addNumericStats(a.plan_lat, b.plan_lat, countA, countB),
    run_lat: addNumericStats(a.run_lat, b.run_lat, countA, countB),
    service_lat: addNumericStats(a.service_lat, b.service_lat, countA, countB),
    overhead_lat: addNumericStats(
      a.overhead_lat,
      b.overhead_lat,
      countA,
      countB,
    ),
    bytes_read: addNumericStats(a.bytes_read, b.bytes_read, countA, countB),
    rows_read: addNumericStats(a.rows_read, b.rows_read, countA, countB),
    sensitive_info: coalesceSensitiveInfo(a.sensitive_info, b.sensitive_info),
    legacy_last_err: "",
    legacy_last_err_redacted: "",
    exec_stats: addExecStats(a.exec_stats, b.exec_stats),
    sql_type: a.sql_type,
    last_exec_timestamp:
      a.last_exec_timestamp.seconds > b.last_exec_timestamp.seconds
        ? a.last_exec_timestamp
        : b.last_exec_timestamp,
    nodes: uniqueLong([...a.nodes, ...b.nodes]),
  };
}

function addExecStats(a: ExecStats, b: ExecStats): Required<ExecStats> {
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
  };
}

function addMaybeUnsetNumericStat(
  a: NumericStat,
  b: NumericStat,
  countA: number,
  countB: number,
): NumericStat {
  return a && b ? addNumericStats(a, b, countA, countB) : null;
}

export function coalesceSensitiveInfo(a: ISensitiveInfo, b: ISensitiveInfo) {
  return {
    last_err: a.last_err || b.last_err,
    most_recent_plan_description:
      a.most_recent_plan_description || b.most_recent_plan_description,
  };
}

export function aggregateStatementStats(
  statementStats: CollectedStatementStatistics[],
) {
  const statementsMap: {
    [statement: string]: CollectedStatementStatistics[];
  } = {};
  statementStats.forEach((statement: CollectedStatementStatistics) => {
    const matches =
      statementsMap[statement.key.key_data.query] ||
      (statementsMap[statement.key.key_data.query] = []);
    matches.push(statement);
  });

  return _.values(statementsMap).map((statements) =>
    _.reduce(
      statements,
      (a: CollectedStatementStatistics, b: CollectedStatementStatistics) => ({
        key: a.key,
        stats: addStatementStats(a.stats, b.stats),
      }),
    ),
  );
}

export interface ExecutionStatistics {
  statement: string;
  app: string;
  database: string;
  distSQL: boolean;
  vec: boolean;
  opt: boolean;
  implicit_txn: boolean;
  full_scan: boolean;
  failed: boolean;
  node_id: number;
  stats: StatementStatistics;
}

export function flattenStatementStats(
  statementStats: CollectedStatementStatistics[],
): ExecutionStatistics[] {
  return statementStats.map((stmt) => ({
    statement: stmt.key.key_data.query,
    app: stmt.key.key_data.app,
    database: stmt.key.key_data.database,
    distSQL: stmt.key.key_data.distSQL,
    vec: stmt.key.key_data.vec,
    opt: stmt.key.key_data.opt,
    implicit_txn: stmt.key.key_data.implicit_txn,
    full_scan: stmt.key.key_data.full_scan,
    failed: stmt.key.key_data.failed,
    node_id: stmt.key.node_id,
    stats: stmt.stats,
  }));
}

export function combineStatementStats(
  statementStats: StatementStatistics[],
): StatementStatistics {
  return _.reduce(statementStats, addStatementStats);
}

// This function returns a key based on all parameters
// that should be used to group statements.
// Parameters being used: node_id, implicit_txn and database.
export function statementKey(stmt: ExecutionStatistics): string {
  return stmt.statement + stmt.implicit_txn + stmt.database;
}
