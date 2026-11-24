// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import Long from "long";

import {
  aggregateNumericStats,
  addExperimentStatsInfo,
  NumericStat,
  flattenStatementStats,
} from "./appStats";

// record is implemented here so we can write the below test as a direct
// analog of the one in pkg/sql/appstatspb/app_stats_test.go.  It's here rather
// than in the main source file because we don't actually need it for the
// application to use.
function record(l: NumericStat, count: number, val: number) {
  const delta = val - l.mean;
  l.mean += delta / count;
  l.squared_diffs += delta * (val - l.mean);
}

function emptyStats() {
  return {
    mean: 0,
    squared_diffs: 0,
  };
}

describe("addNumericStats", () => {
  it("adds two numeric stats together", () => {
    const aData = [1.1, 3.3, 2.2];
    const bData = [2.0, 3.0, 5.5, 1.2];

    let countA = 0;
    let countB = 0;
    let countAB = 0;

    let sumA = 0;
    let sumB = 0;
    let sumAB = 0;

    const a = emptyStats();
    const b = emptyStats();
    const ab = emptyStats();

    aData.forEach(v => {
      countA++;
      sumA += v;
      record(a, countA, v);
    });

    bData.forEach(v => {
      countB++;
      sumB += v;
      record(b, countB, v);
    });

    bData.concat(aData).forEach(v => {
      countAB++;
      sumAB += v;
      record(ab, countAB, v);
    });

    expect(a.mean).toBeCloseTo(2.2, 6);
    expect(a.mean).toBeCloseTo(sumA / countA, 6);
    expect(b.mean).toBeCloseTo(sumB / countB, 6);
    expect(ab.mean).toBeCloseTo(sumAB / countAB, 6);

    const combined = aggregateNumericStats(a, b, countA, countB);

    expect(combined.mean).toBeCloseTo(ab.mean, 6);
    expect(combined.squared_diffs).toBeCloseTo(ab.squared_diffs, 6);

    const reversed = aggregateNumericStats(b, a, countB, countA);

    expect(reversed).toEqual(combined);
  });
});

describe("addExperimentStatsInfo", () => {
  it("returns empty object when both inputs are null/undefined", () => {
    expect(addExperimentStatsInfo(null, null)).toEqual({});
    expect(addExperimentStatsInfo(undefined, undefined)).toEqual({});
  });

  it("returns b when a is null", () => {
    const b = {
      count: Long.fromInt(3),
      run_lat: { mean: 1.5, squared_diffs: 0.5 },
      plan_lat: { mean: 0.8, squared_diffs: 0.2 },
    };
    expect(addExperimentStatsInfo(null, b)).toBe(b);
  });

  it("returns a when b is null", () => {
    const a = {
      count: Long.fromInt(2),
      run_lat: { mean: 1.0, squared_diffs: 0.3 },
      plan_lat: { mean: 0.5, squared_diffs: 0.1 },
    };
    expect(addExperimentStatsInfo(a, null)).toBe(a);
  });

  it("aggregates two non-null inputs", () => {
    const a = {
      count: Long.fromInt(3),
      run_lat: { mean: 2.0, squared_diffs: 1.0 },
      plan_lat: { mean: 1.0, squared_diffs: 0.5 },
    };
    const b = {
      count: Long.fromInt(2),
      run_lat: { mean: 3.0, squared_diffs: 0.5 },
      plan_lat: { mean: 1.5, squared_diffs: 0.2 },
    };

    const result = addExperimentStatsInfo(a, b);

    // count should be summed.
    expect(result.count.toInt()).toBe(5);

    // run_lat and plan_lat should produce finite, non-NaN values.
    expect(Number.isFinite(result.run_lat.mean)).toBe(true);
    expect(Number.isNaN(result.run_lat.mean)).toBe(false);
    expect(Number.isFinite(result.run_lat.squared_diffs)).toBe(true);
    expect(Number.isNaN(result.run_lat.squared_diffs)).toBe(false);

    expect(Number.isFinite(result.plan_lat.mean)).toBe(true);
    expect(Number.isNaN(result.plan_lat.mean)).toBe(false);
    expect(Number.isFinite(result.plan_lat.squared_diffs)).toBe(true);
    expect(Number.isNaN(result.plan_lat.squared_diffs)).toBe(false);

    // Weighted mean: (2.0*3 + 3.0*2) / 5 = 2.4
    expect(result.run_lat.mean).toBeCloseTo(2.4, 6);
    // Weighted mean: (1.0*3 + 1.5*2) / 5 = 1.2
    expect(result.plan_lat.mean).toBeCloseTo(1.2, 6);
  });

  it("handles zero counts without producing NaN", () => {
    const a = {
      count: Long.fromInt(0),
      run_lat: { mean: 0, squared_diffs: 0 },
      plan_lat: { mean: 0, squared_diffs: 0 },
    };
    const b = {
      count: Long.fromInt(0),
      run_lat: { mean: 0, squared_diffs: 0 },
      plan_lat: { mean: 0, squared_diffs: 0 },
    };

    const result = addExperimentStatsInfo(a, b);

    expect(result.count.toInt()).toBe(0);
    // With zero counts the function uses countA || 1 / countB || 1
    // to avoid division by zero — verify no NaN.
    expect(Number.isNaN(result.run_lat.mean)).toBe(false);
    expect(Number.isNaN(result.plan_lat.mean)).toBe(false);
  });
});

describe("flattenStatementStats", () => {
  it("flattens CollectedStatementStatistics to ExecutionStatistics", () => {
    const stats = [
      {
        key: {
          key_data: {
            query: "SELECT * FROM foobar",
            query_summary: "SELECT * FROM foobar",
            app: "foobar",
            distSQL: true,
            vec: false,
            opt: true,
            full_scan: true,
            failed: false,
          },
          node_id: 1,
        },
        stats: {},
      },
      {
        key: {
          key_data: {
            query: "UPDATE foobar SET name = 'baz' WHERE id = 42",
            query_summary: "UPDATE foobar SET name = 'baz' WHERE id = 42",
            app: "bazzer",
            distSQL: false,
            vec: false,
            opt: false,
            full_scan: false,
            failed: true,
          },
          node_id: 2,
        },
        stats: {},
      },
      {
        key: {
          key_data: {
            query:
              "SELECT app_name, aggregated_ts, fingerprint_id, metadata, statistics FROM system.app_statistics JOIN system.transaction_statistics ON crdb_internal.transaction_statistics.app_name = system.transaction_statistics.app_name",
            query_summary:
              "SELECT app_name, aggre... FROM system.app_statistics JOIN sys...",
            app: "unique_pear",
            distSQL: false,
            vec: false,
            opt: false,
            full_scan: true,
            failed: true,
          },
          node_id: 3,
        },
        stats: {},
      },
      {
        key: {
          key_data: {
            query:
              "INSERT INTO system.public.lease(\"descID\", version, \"nodeID\", expiration) VALUES ('1232', '111', __more1_10__)",
            query_summary:
              'INSERT INTO system.public.lease("descID", versi...)',
            app: "test_summary",
            distSQL: false,
            vec: false,
            opt: false,
            full_scan: false,
            failed: true,
          },
          node_id: 4,
        },
        stats: {},
      },
      {
        key: {
          key_data: {
            query:
              "UPDATE system.jobs SET status = $2, payload = $3, last_run = $4, num_runs = $5 WHERE internal_table_id = $1",
            query_summary:
              "UPDATE system.jobs SET status = $2, pa... WHERE internal_table_...",
            app: "test1",
            distSQL: false,
            vec: false,
            opt: false,
            full_scan: false,
            failed: true,
          },
          node_id: 5,
        },
        stats: {},
      },
    ];

    const flattened = flattenStatementStats(stats);

    expect(flattened).toHaveLength(stats.length);

    for (let i = 0; i < flattened.length; i++) {
      expect(flattened[i].statement).toBe(stats[i].key.key_data.query);
      expect(flattened[i].statement_summary).toBe(
        stats[i].key.key_data.query_summary,
      );
      expect(flattened[i].app).toBe(stats[i].key.key_data.app);
      expect(flattened[i].distSQL).toBe(stats[i].key.key_data.distSQL);
      expect(flattened[i].vec).toBe(stats[i].key.key_data.vec);
      expect(flattened[i].full_scan).toBe(stats[i].key.key_data.full_scan);
      expect(flattened[i].node_id).toBe(stats[i].key.node_id);

      expect(flattened[i].stats).toBe(stats[i].stats);
    }
  });
});
