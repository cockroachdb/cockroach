// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { AlignedData } from "uplot";

import { longToInt, TimestampToNumber } from "../util";

type StatementStatisticsPerAggregatedTs =
  cockroach.server.serverpb.StatementDetailsResponse.ICollectedStatementGroupedByAggregatedTs;

export function generateExecuteAndPlanningTimeseries(
  stats: StatementStatisticsPerAggregatedTs[],
): AlignedData {
  const ts: Array<number> = [];
  const execution: Array<number> = [];
  const planning: Array<number> = [];

  stats.forEach(function (stat: StatementStatisticsPerAggregatedTs) {
    ts.push(TimestampToNumber(stat.aggregated_ts) * 1e3);
    execution.push(stat.stats.run_lat.mean * 1e9);
    planning.push(stat.stats.plan_lat.mean * 1e9);
  });

  return [ts, execution, planning];
}

export function generateClientWaitTimeseries(
  stats: StatementStatisticsPerAggregatedTs[],
): AlignedData {
  const ts: Array<number> = [];
  const clientWait: Array<number> = [];

  stats.forEach(function (stat: StatementStatisticsPerAggregatedTs) {
    ts.push(TimestampToNumber(stat.aggregated_ts) * 1e3);
    clientWait.push(stat.stats.idle_lat.mean * 1e9);
  });

  return [ts, clientWait];
}

export function generateRowsProcessedTimeseries(
  stats: StatementStatisticsPerAggregatedTs[],
): AlignedData {
  const ts: Array<number> = [];
  const read: Array<number> = [];
  const written: Array<number> = [];

  stats.forEach(function (stat: StatementStatisticsPerAggregatedTs) {
    ts.push(TimestampToNumber(stat.aggregated_ts) * 1e3);
    read.push(stat.stats.rows_read?.mean);
    written.push(stat.stats.rows_written?.mean);
  });

  return [ts, read, written];
}

export function generateExecRetriesTimeseries(
  stats: StatementStatisticsPerAggregatedTs[],
): AlignedData {
  const ts: Array<number> = [];
  const retries: Array<number> = [];

  stats.forEach(function (stat: StatementStatisticsPerAggregatedTs) {
    ts.push(TimestampToNumber(stat.aggregated_ts) * 1e3);

    const totalCountBarChart = longToInt(stat.stats.count);
    const firstAttemptsBarChart = longToInt(stat.stats.first_attempt_count);
    retries.push(totalCountBarChart - firstAttemptsBarChart);
  });

  return [ts, retries];
}

export function generateExecCountTimeseries(
  stats: StatementStatisticsPerAggregatedTs[],
): AlignedData {
  const ts: Array<number> = [];
  const count: Array<number> = [];

  stats.forEach(function (stat: StatementStatisticsPerAggregatedTs) {
    ts.push(TimestampToNumber(stat.aggregated_ts) * 1e3);
    count.push(longToInt(stat.stats.count));
  });

  return [ts, count];
}

export function generateContentionTimeseries(
  stats: StatementStatisticsPerAggregatedTs[],
): AlignedData {
  const ts: Array<number> = [];
  const count: Array<number> = [];

  stats.forEach(function (stat: StatementStatisticsPerAggregatedTs) {
    ts.push(TimestampToNumber(stat.aggregated_ts) * 1e3);
    count.push(stat.stats.exec_stats.contention_time.mean * 1e9);
  });

  return [ts, count];
}

export function generateCPUTimeseries(
  stats: StatementStatisticsPerAggregatedTs[],
): AlignedData {
  const ts: Array<number> = [];
  const count: Array<number> = [];

  stats.forEach(function (stat: StatementStatisticsPerAggregatedTs) {
    if (stat.stats.exec_stats.cpu_sql_nanos) {
      ts.push(TimestampToNumber(stat.aggregated_ts) * 1e3);
      count.push(stat.stats.exec_stats.cpu_sql_nanos.mean);
    }
  });

  return [ts, count];
}

type StatementStatisticsPerAggregatedTsAndPlanHash =
  cockroach.server.serverpb.StatementDetailsResponse.IStatementPlanDistribution;
export function generatePlanDistributionTimeseries(
  stats: StatementStatisticsPerAggregatedTsAndPlanHash[],
): { alignedData: AlignedData; planGists: string[] } {
  const timeMap = new Map<number, Map<string, number>>();
  const planGistSet = new Set<string>();

  stats.forEach(stat => {
    const ts = TimestampToNumber(stat.aggregated_ts) * 1e3;
    const planGist = stat.plan_gist || "unknown";
    planGistSet.add(planGist);

    if (!timeMap.has(ts)) {
      timeMap.set(ts, new Map());
    }

    const existingCount = timeMap.get(ts).get(planGist) || 0;
    timeMap
      .get(ts)
      .set(planGist, existingCount + Number(stat.execution_count || 0));
  });

  const timestamps = Array.from(timeMap.keys()).sort((a, b) => a - b);
  const planGists = Array.from(planGistSet).sort();

  // Build aligned data structure: [timestamps, plan1_counts, plan2_counts, ...]
  const alignedData: AlignedData = [timestamps];

  planGists.forEach(planGist => {
    const counts = timestamps.map(ts => {
      return timeMap.get(ts)?.get(planGist) || 0;
    });
    alignedData.push(counts);
  });

  return { alignedData, planGists };
}
