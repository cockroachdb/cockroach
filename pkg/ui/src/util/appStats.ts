// This file significantly duplicates the algorithms available in
// pkg/roachpb/app_stats.go, in particular the functions on NumericStats
// to compute variance and add together NumericStats.

import _ from "lodash";
import * as protos from "src/js/protos";
import { FixLong } from "src/util/fixLong";

export type StatementStatistics = protos.cockroach.sql.StatementStatistics$Properties;
export type CollectedStatementStatistics = protos.cockroach.sql.CollectedStatementStatistics$Properties;

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

export function addNumericStats(a: NumericStat, b: NumericStat, countA: number, countB: number) {
  const total = countA + countB;
  const delta = b.mean - a.mean;

  return {
    mean: ((a.mean * countA) + (b.mean * countB)) / total,
    squared_diffs: (a.squared_diffs + b.squared_diffs) + delta * delta * countA * countB / total,
  };
}

export function addStatementStats(a: StatementStatistics, b: StatementStatistics) {
  const countA = FixLong(a.count).toInt();
  const countB = FixLong(b.count).toInt();
  return {
    count: a.count.add(b.count),
    first_attempt_count: a.first_attempt_count.add(b.first_attempt_count),
    max_retries: a.max_retries.greaterThan(b.max_retries) ? a.max_retries : b.max_retries,
    num_rows: addNumericStats(a.num_rows, b.num_rows, countA, countB),
    parse_lat: addNumericStats(a.parse_lat, b.parse_lat, countA, countB),
    plan_lat: addNumericStats(a.plan_lat, b.plan_lat, countA, countB),
    run_lat: addNumericStats(a.run_lat, b.run_lat, countA, countB),
    service_lat: addNumericStats(a.service_lat, b.service_lat, countA, countB),
    overhead_lat: addNumericStats(a.overhead_lat, b.overhead_lat, countA, countB),
  };
}

export function aggregateStatementStats(statementStats: CollectedStatementStatistics[]) {
  const statementsMap: { [query: string]: CollectedStatementStatistics[] } = {};
  statementStats.forEach(
    (statement: CollectedStatementStatistics) => {
      const matches = statementsMap[statement.key.query] || (statementsMap[statement.key.query] = []);
      matches.push(statement);
  });

  return _.values(statementsMap).map(statements =>
    _.reduce(statements, (a: CollectedStatementStatistics, b: CollectedStatementStatistics) => ({
      key: a.key,
      stats: addStatementStats(a.stats, b.stats),
    })),
  );
}

export interface ExecutionStatistics {
  statement: string;
  app: string;
  distSQL: boolean;
  failed: boolean;
  stats: StatementStatistics;
}

export function flattenStatementStats(statementStats: CollectedStatementStatistics[]): ExecutionStatistics[] {
  return statementStats.map(stmt => ({
    statement: stmt.key.query,
    app: stmt.key.app,
    distSQL: stmt.key.distSQL,
    failed: stmt.key.failed,
    stats: stmt.stats,
  }));
}

export function combineStatementStats(statementStats: StatementStatistics[]): StatementStatistics {
  return _.reduce(statementStats, addStatementStats);
}
