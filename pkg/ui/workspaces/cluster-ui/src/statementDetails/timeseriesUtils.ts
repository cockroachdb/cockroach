// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";

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

// generateCanaryVsStableTimeseries builds a grouped+stacked bar chart.
// For each aggregated timestamp, two side-by-side bars are shown: one
// for canary and one for stable. Each bar is split into execution
// (bottom) and planning (top) portions, matching the color order of
// the "Statement Times" chart (execution = blue at index 0, planning =
// green at index 1).
//
// Both canary and stable latencies are tracked explicitly in the backend
// rather than deriving stable from (total - canary), because executions
// where the canary experiment is off should not be counted as stable.
export function generateCanaryVsStableTimeseries(
  stats: StatementStatisticsPerAggregatedTs[],
): GroupedBarData {
  return stats
    .filter(stat => {
      const canaryCount = longToInt(stat.stats.canary_stats?.count);
      const stableCount = longToInt(stat.stats.stable_stats?.count);
      return canaryCount > 0 || stableCount > 0;
    })
    .map(stat => ({
      timestamp: TimestampToNumber(stat.aggregated_ts) * 1e3,
      groups: [
        {
          label: "Canary",
          layers: [
            {
              label: "Execution",
              value: (stat.stats.canary_stats?.run_lat?.mean ?? 0) * 1e9,
              color: SERIES_PALETTE[0],
            },
            {
              label: "Planning",
              value: (stat.stats.canary_stats?.plan_lat?.mean ?? 0) * 1e9,
              color: SERIES_PALETTE[1],
            },
          ],
        },
        {
          label: "Stable",
          layers: [
            {
              label: "Execution",
              value: (stat.stats.stable_stats?.run_lat?.mean ?? 0) * 1e9,
              color: SERIES_PALETTE[0],
            },
            {
              label: "Planning",
              value: (stat.stats.stable_stats?.plan_lat?.mean ?? 0) * 1e9,
              color: SERIES_PALETTE[1],
            },
          ],
        },
      ],
    }));
}

// HSL parameters for the latency heat-map palette. These should stay in
// sync with the gradient in .latency-legend__bar (statementDetails.module.scss).
const LATENCY_HUE = 261;
const LATENCY_SATURATION = 97;
const LATENCY_LIGHTNESS_MIN = 35; // darkest (highest latency)
const LATENCY_LIGHTNESS_MAX = 85; // lightest (lowest latency)

// latencyToColor maps a latency value to a purple-tone HSL color.
// Lower latency → lighter (higher lightness), higher → darker.
function latencyToColor(lat: number, min: number, max: number): string {
  const t = max === min ? 0.5 : (lat - min) / (max - min);
  const lightness =
    LATENCY_LIGHTNESS_MAX - t * (LATENCY_LIGHTNESS_MAX - LATENCY_LIGHTNESS_MIN);
  return `hsl(${LATENCY_HUE}, ${LATENCY_SATURATION}%, ${lightness}%)`;
}

// generateCanaryVsStablePlanDistribution builds a grouped bar chart
// with two bars per timestamp: canary (left) and stable (right).
// Each bar is stacked by plan gist, showing execution counts.
//
// When latencyByGist is provided, plan gists are sorted by latency
// ascending (lowest latency at the bottom of the stack) and each gist
// is assigned a heat-map color (light purple = low latency, dark
// purple = high latency). The latency range is returned so the caller
// can render a legend.
export function generateCanaryVsStablePlanDistribution(
  stats: StatementStatisticsPerAggregatedTsAndPlanHash[],
  latencyByGist?: Map<string, number>,
): { data: GroupedBarData; latencyRange?: { min: number; max: number } } {
  // Collect unique plan gists.
  const planGistSet = new Set<string>();
  stats.forEach(stat => {
    const gist = stat.plan_gist || "unknown";
    planGistSet.add(gist);
  });

  // Sort and color plan gists. When latency data is available, sort
  // by latency ascending and use a purple heat-map. Otherwise fall
  // back to evenly-spaced purple shades.
  const planGistArray = Array.from(planGistSet);
  let planGists: string[];
  let latencyRange: { min: number; max: number } | undefined;
  const gistColors = new Map<string, string>();

  const hasLatencyData =
    latencyByGist &&
    latencyByGist.size > 0 &&
    planGistArray.some(g => latencyByGist.has(g));

  if (hasLatencyData) {
    planGists = planGistArray.sort(
      (a, b) => (latencyByGist.get(a) || 0) - (latencyByGist.get(b) || 0),
    );
    const lats = planGists.map(g => latencyByGist.get(g) || 0);
    const minLat = Math.min(...lats);
    const maxLat = Math.max(...lats);
    planGists.forEach((g, i) => {
      gistColors.set(g, latencyToColor(lats[i], minLat, maxLat));
    });
    latencyRange = { min: minLat, max: maxLat };
  } else {
    planGists = planGistArray.sort();
    const n = planGists.length;
    planGists.forEach((gist, i) => {
      // Use index as a synthetic "latency" so latencyToColor spreads
      // colors evenly across the palette. When n<=1, min===max so
      // latencyToColor uses its midpoint fallback (t=0.5).
      gistColors.set(gist, latencyToColor(i, 0, n - 1));
    });
  }

  // Group stats by timestamp.
  type GistCounts = {
    canary: Map<string, number>;
    stable: Map<string, number>;
  };
  const timeMap = new Map<number, GistCounts>();

  stats.forEach(stat => {
    const ts = TimestampToNumber(stat.aggregated_ts) * 1e3;
    const gist = stat.plan_gist || "unknown";

    if (!timeMap.has(ts)) {
      timeMap.set(ts, {
        canary: new Map(),
        stable: new Map(),
      });
    }
    const entry = timeMap.get(ts);

    const canaryCount = Number(stat.canary_execution_count || 0);
    const stableCount = Number(stat.stable_execution_count || 0);

    if (canaryCount > 0 || stableCount > 0) {
      entry.canary.set(gist, (entry.canary.get(gist) || 0) + canaryCount);
      entry.stable.set(gist, (entry.stable.get(gist) || 0) + stableCount);
    }
  });

  // Filter to timestamps that have any canary/stable data.
  const timestamps = Array.from(timeMap.keys())
    .filter(ts => {
      const entry = timeMap.get(ts);
      return (
        [...entry.canary.values()].some(v => v > 0) ||
        [...entry.stable.values()].some(v => v > 0)
      );
    })
    .sort((a, b) => a - b);

  const data: GroupedBarData = timestamps.map(ts => {
    const entry = timeMap.get(ts);
    return {
      timestamp: ts,
      groups: [
        {
          label: "Canary",
          layers: planGists.map(gist => ({
            label: `Plan ${gist}`,
            value: entry.canary.get(gist) || 0,
            color: gistColors.get(gist),
          })),
        },
        {
          label: "Stable",
          layers: planGists.map(gist => ({
            label: `Plan ${gist}`,
            value: entry.stable.get(gist) || 0,
            color: gistColors.get(gist),
          })),
        },
      ],
    };
  });

  return { data, latencyRange };
}
