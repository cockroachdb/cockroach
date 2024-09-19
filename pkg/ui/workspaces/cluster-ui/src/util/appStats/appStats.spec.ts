// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { assert } from "chai";

import {
  aggregateNumericStats,
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

    assert.approximately(a.mean, 2.2, 0.0000001);
    assert.approximately(a.mean, sumA / countA, 0.0000001);
    assert.approximately(b.mean, sumB / countB, 0.0000001);
    assert.approximately(ab.mean, sumAB / countAB, 0.0000001);

    const combined = aggregateNumericStats(a, b, countA, countB);

    assert.approximately(combined.mean, ab.mean, 0.0000001);
    assert.approximately(combined.squared_diffs, ab.squared_diffs, 0.0000001);

    const reversed = aggregateNumericStats(b, a, countB, countA);

    assert.deepEqual(reversed, combined);
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

    assert.equal(flattened.length, stats.length);

    for (let i = 0; i < flattened.length; i++) {
      assert.equal(flattened[i].statement, stats[i].key.key_data.query);
      assert.equal(
        flattened[i].statement_summary,
        stats[i].key.key_data.query_summary,
      );
      assert.equal(flattened[i].app, stats[i].key.key_data.app);
      assert.equal(flattened[i].distSQL, stats[i].key.key_data.distSQL);
      assert.equal(flattened[i].vec, stats[i].key.key_data.vec);
      assert.equal(flattened[i].full_scan, stats[i].key.key_data.full_scan);
      assert.equal(flattened[i].node_id, stats[i].key.node_id);

      assert.equal(flattened[i].stats, stats[i].stats);
    }
  });
});
