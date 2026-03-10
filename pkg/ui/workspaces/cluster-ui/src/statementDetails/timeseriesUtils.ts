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

// generateCanaryVsStableTimeseries builds a time series with four data
// series for a grouped+stacked bar chart. For each aggregated timestamp,
// two side-by-side bars are shown: one for canary and one for stable.
// Each bar is split into execution (bottom) and planning (top) portions,
// matching the color order of the "Statement Times" chart (execution =
// blue at index 0, planning = green at index 1).
//
// Returns [timestamps, canaryRun, canaryPlan, stableRun, stablePlan].
// Both canary and stable latencies are tracked explicitly in the backend
// rather than deriving stable from (total - canary), because executions
// where the canary experiment is off should not be counted as stable.
export function generateCanaryVsStableTimeseries(
  stats: StatementStatisticsPerAggregatedTs[],
): AlignedData {
  const ts: Array<number> = [];
  const canaryRun: Array<number> = [];
  const canaryPlan: Array<number> = [];
  const stableRun: Array<number> = [];
  const stablePlan: Array<number> = [];

  stats.forEach(function (stat: StatementStatisticsPerAggregatedTs) {
    const canaryStats = stat.stats?.canary_stats;
    const stableStats = stat.stats?.stable_stats;
    const canaryCount = longToInt(canaryStats?.count) || 0;
    const stableCount = longToInt(stableStats?.count) || 0;

    // Skip timestamps where the canary experiment was not active.
    if (canaryCount === 0 && stableCount === 0) {
      return;
    }

    ts.push(TimestampToNumber(stat.aggregated_ts) * 1e3);

    canaryRun.push((canaryStats?.run_lat?.mean || 0) * 1e9);
    canaryPlan.push((canaryStats?.plan_lat?.mean || 0) * 1e9);
    stableRun.push((stableStats?.run_lat?.mean || 0) * 1e9);
    stablePlan.push((stableStats?.plan_lat?.mean || 0) * 1e9);
  });

  return [ts, canaryRun, canaryPlan, stableRun, stablePlan];
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

// generateCanaryVsStablePlanDistributionTimeseries builds a grouped bar
// chart with two bars per timestamp: canary (left) and stable (right).
// Each bar is stacked by plan gist, showing execution counts.
//
// Returns data as [timestamps, gist1_canary, gist2_canary, ..., gist1_stable, gist2_stable, ...]
// along with the plan gist list and the number of gists (groupSize).
export function generateCanaryVsStablePlanDistributionTimeseries(
  stats: StatementStatisticsPerAggregatedTsAndPlanHash[],
): { alignedData: AlignedData; planGists: string[]; groupSize: number } {
  // Two maps: one for canary counts, one for stable counts.
  const canaryMap = new Map<number, Map<string, number>>();
  const stableMap = new Map<number, Map<string, number>>();
  const planGistSet = new Set<string>();
  let hasCanary = false;

  stats.forEach(stat => {
    const ts = TimestampToNumber(stat.aggregated_ts) * 1e3;
    const planGist = stat.plan_gist || "unknown";
    planGistSet.add(planGist);

    const canaryCount = Number(stat.canary_execution_count || 0);
    // Use the explicitly tracked stable count rather than deriving it
    // from (total - canary), since executions where the canary experiment
    // is off should not be counted as stable.
    const stableCount = Number(stat.stable_execution_count || 0);

    if (canaryCount > 0 || stableCount > 0) {
      hasCanary = true;
    }

    if (!canaryMap.has(ts)) {
      canaryMap.set(ts, new Map());
      stableMap.set(ts, new Map());
    }

    const existingCanary = canaryMap.get(ts).get(planGist) || 0;
    canaryMap.get(ts).set(planGist, existingCanary + canaryCount);

    const existingStable = stableMap.get(ts).get(planGist) || 0;
    stableMap.get(ts).set(planGist, existingStable + stableCount);
  });

  if (!hasCanary) {
    return { alignedData: [[]], planGists: [], groupSize: 0 };
  }

  const timestamps = Array.from(canaryMap.keys()).sort((a, b) => a - b);
  const planGists = Array.from(planGistSet).sort();
  const groupSize = planGists.length;

  // Data layout: [timestamps, ...canary_gists, ...stable_gists]
  const alignedData: AlignedData = [timestamps];

  // Canary series (one per plan gist)
  planGists.forEach(planGist => {
    alignedData.push(
      timestamps.map(ts => canaryMap.get(ts)?.get(planGist) || 0),
    );
  });

  // Stable series (one per plan gist)
  planGists.forEach(planGist => {
    alignedData.push(
      timestamps.map(ts => stableMap.get(ts)?.get(planGist) || 0),
    );
  });

  return { alignedData, planGists, groupSize };
}
