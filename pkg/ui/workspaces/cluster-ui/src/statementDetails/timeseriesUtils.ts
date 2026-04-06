// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { AlignedData } from "uplot";

import { GroupedBarData } from "../graphs/groupedBarChart";
import { longToInt, TimestampToNumber } from "../util";

type StatementStatisticsPerAggregatedTs =
  cockroach.server.serverpb.StatementDetailsResponse.ICollectedStatementGroupedByAggregatedTs;

// Default color palette matching the existing uPlot bar chart series colors.
const SERIES_PALETTE = [
  "#003EBD",
  "#2AAF44",
  "#F16969",
  "#4E9FD1",
  "#49D990",
  "#D77FBF",
  "#87326D",
  "#A3415B",
  "#B59153",
  "#C9DB6D",
  "#475872",
  "#748BF2",
  "#91C8F2",
  "#FF9696",
  "#EF843C",
  "#DCCD4B",
];

export function generateExecuteAndPlanningTimeseries(
  stats: StatementStatisticsPerAggregatedTs[],
): GroupedBarData {
  return stats.map(stat => ({
    timestamp: TimestampToNumber(stat.aggregated_ts) * 1e3,
    groups: [
      {
        label: "",
        layers: [
          {
            label: "Execution",
            value: (stat.stats.run_lat?.mean ?? 0) * 1e9,
            color: SERIES_PALETTE[0],
          },
          {
            label: "Planning",
            value: (stat.stats.plan_lat?.mean ?? 0) * 1e9,
            color: SERIES_PALETTE[1],
          },
        ],
      },
    ],
  }));
}

export function generateClientWaitTimeseries(
  stats: StatementStatisticsPerAggregatedTs[],
): GroupedBarData {
  return stats.map(stat => ({
    timestamp: TimestampToNumber(stat.aggregated_ts) * 1e3,
    groups: [
      {
        label: "",
        layers: [
          {
            label: "Client Wait Time",
            value: (stat.stats.idle_lat?.mean ?? 0) * 1e9,
            color: SERIES_PALETTE[0],
          },
        ],
      },
    ],
  }));
}

export function generateRowsProcessedTimeseries(
  stats: StatementStatisticsPerAggregatedTs[],
): GroupedBarData {
  return stats.map(stat => ({
    timestamp: TimestampToNumber(stat.aggregated_ts) * 1e3,
    groups: [
      {
        label: "",
        layers: [
          {
            label: "Rows Read",
            value: stat.stats.rows_read?.mean ?? 0,
            color: SERIES_PALETTE[0],
          },
          {
            label: "Rows Written",
            value: stat.stats.rows_written?.mean ?? 0,
            color: SERIES_PALETTE[1],
          },
        ],
      },
    ],
  }));
}

export function generateExecRetriesTimeseries(
  stats: StatementStatisticsPerAggregatedTs[],
): GroupedBarData {
  return stats.map(stat => {
    const totalCount = longToInt(stat.stats.count);
    const firstAttempts = longToInt(stat.stats.first_attempt_count);
    return {
      timestamp: TimestampToNumber(stat.aggregated_ts) * 1e3,
      groups: [
        {
          label: "",
          layers: [
            {
              label: "Retries",
              value: totalCount - firstAttempts,
              color: SERIES_PALETTE[0],
            },
          ],
        },
      ],
    };
  });
}

export function generateExecCountTimeseries(
  stats: StatementStatisticsPerAggregatedTs[],
): GroupedBarData {
  return stats.map(stat => ({
    timestamp: TimestampToNumber(stat.aggregated_ts) * 1e3,
    groups: [
      {
        label: "",
        layers: [
          {
            label: "Execution Counts",
            value: longToInt(stat.stats.count),
            color: SERIES_PALETTE[0],
          },
        ],
      },
    ],
  }));
}

export function generateContentionTimeseries(
  stats: StatementStatisticsPerAggregatedTs[],
): GroupedBarData {
  return stats.map(stat => ({
    timestamp: TimestampToNumber(stat.aggregated_ts) * 1e3,
    groups: [
      {
        label: "",
        layers: [
          {
            label: "Contention",
            value: (stat.stats.exec_stats?.contention_time?.mean ?? 0) * 1e9,
            color: SERIES_PALETTE[0],
          },
        ],
      },
    ],
  }));
}

export function generateCPUTimeseries(
  stats: StatementStatisticsPerAggregatedTs[],
): GroupedBarData {
  return stats
    .filter(stat => stat.stats.exec_stats?.cpu_sql_nanos != null)
    .map(stat => ({
      timestamp: TimestampToNumber(stat.aggregated_ts) * 1e3,
      groups: [
        {
          label: "",
          layers: [
            {
              label: "SQL CPU Time",
              value: stat.stats.exec_stats?.cpu_sql_nanos?.mean ?? 0,
              color: SERIES_PALETTE[0],
            },
          ],
        },
      ],
    }));
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
): { data: GroupedBarData; planGists: string[] } {
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

  // Assign a color to each plan gist.
  const gistColors = new Map<string, string>();
  planGists.forEach((gist, i) => {
    gistColors.set(gist, SERIES_PALETTE[i % SERIES_PALETTE.length]);
  });

  const data: GroupedBarData = timestamps.map(ts => ({
    timestamp: ts,
    groups: [
      {
        label: "",
        layers: planGists.map(gist => ({
          label: `Plan ${gist}`,
          value: timeMap.get(ts)?.get(gist) || 0,
          color: gistColors.get(gist),
        })),
      },
    ],
  }));

  return { data, planGists };
}

// latencyToColor maps a latency value to a purple-tone HSL color.
// Lower latency → lighter (higher lightness), higher → darker.
function latencyToColor(lat: number, min: number, max: number): string {
  const t = max === min ? 0.5 : (lat - min) / (max - min);
  // Lightness ranges from 85% (light purple) to 35% (dark purple).
  const lightness = 85 - t * 50;
  return `hsl(261, 97%, ${lightness}%)`;
}

// generateCanaryVsStablePlanDistributionTimeseries builds a grouped bar
// chart with two bars per timestamp: canary (left) and stable (right).
// Each bar is stacked by plan gist, showing execution counts.
//
// When latencyByGist is provided, plan gists are sorted by latency
// ascending (lowest latency at the bottom of the stack) and each gist
// is assigned a heat-map color (light purple = low latency, dark
// purple = high latency). The colour palette and latency range are
// returned so the caller can render a legend.
//
// Returns data as [timestamps, gist1_canary, gist2_canary, ..., gist1_stable, gist2_stable, ...]
// along with the plan gist list and the number of gists (groupSize).
export function generateCanaryVsStablePlanDistributionTimeseries(
  stats: StatementStatisticsPerAggregatedTsAndPlanHash[],
  latencyByGist?: Map<string, number>,
): {
  alignedData: AlignedData;
  planGists: string[];
  groupSize: number;
  colourPalette: string[];
  latencyRange?: { min: number; max: number };
} {
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
    return {
      alignedData: [[]],
      planGists: [],
      groupSize: 0,
      colourPalette: [],
    };
  }

  const timestamps = Array.from(canaryMap.keys()).sort((a, b) => a - b);

  // Sort plan gists by latency ascending when a latency map is
  // provided, so lower-latency plans sit at the bottom of the stack
  // and receive lighter colors. Always generate a purple heat-map
  // palette — if no latency data is available, use evenly-spaced
  // purple shades so the chart never falls back to the default
  // multi-colour palette.
  let planGists: string[];
  let colourPalette: string[];
  let latencyRange: { min: number; max: number } | undefined;

  // Check whether latencyByGist has entries that actually match gists
  // in this chart's data.
  const hasLatencyData =
    latencyByGist &&
    latencyByGist.size > 0 &&
    Array.from(planGistSet).some(g => latencyByGist.has(g));

  if (hasLatencyData) {
    planGists = Array.from(planGistSet).sort((a, b) => {
      return (latencyByGist.get(a) || 0) - (latencyByGist.get(b) || 0);
    });
    const lats = planGists.map(g => latencyByGist.get(g) || 0);
    const minLat = Math.min(...lats);
    const maxLat = Math.max(...lats);
    colourPalette = planGists.map(g =>
      latencyToColor(latencyByGist.get(g) || 0, minLat, maxLat),
    );
    latencyRange = { min: minLat, max: maxLat };
  } else {
    planGists = Array.from(planGistSet).sort();
    // Evenly-spaced purple shades as fallback.
    const n = planGists.length;
    colourPalette = planGists.map((_, i) => {
      const t = n <= 1 ? 0.5 : i / (n - 1);
      const lightness = 85 - t * 50;
      return `hsl(261, 97%, ${lightness}%)`;
    });
  }

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

  return { alignedData, planGists, groupSize, colourPalette, latencyRange };
}
