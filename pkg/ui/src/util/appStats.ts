// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

// This file significantly duplicates the algorithms available in
// pkg/roachpb/app_stats.go, in particular the functions on NumericStats
// to compute variance and add together NumericStats.

import _ from "lodash";
import * as protos from "src/js/protos";
import { FixLong } from "src/util/fixLong";

export type StatementStatistics = protos.cockroach.sql.IStatementStatistics;
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
  const statementsMap: { [statement: string]: CollectedStatementStatistics[] } = {};
  statementStats.forEach(
    (statement: CollectedStatementStatistics) => {
      const matches = statementsMap[statement.key.key_data.query] || (statementsMap[statement.key.key_data.query] = []);
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
  opt: boolean;
  failed: boolean;
  node_id: number;
  stats: StatementStatistics;
}

export function flattenStatementStats(statementStats: CollectedStatementStatistics[]): ExecutionStatistics[] {
  return statementStats.map(stmt => ({
    statement: stmt.key.key_data.query,
    app:       stmt.key.key_data.app,
    distSQL:   stmt.key.key_data.distSQL,
    opt:       stmt.key.key_data.opt,
    failed:    stmt.key.key_data.failed,
    node_id:   stmt.key.node_id,
    stats:     stmt.stats,
  }));
}

export function combineStatementStats(statementStats: StatementStatistics[]): StatementStatistics {
  return _.reduce(statementStats, addStatementStats);
}
