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

import { assert } from "chai";
import Long from "long";

import { addNumericStats, NumericStat, flattenStatementStats, StatementStatistics, combineStatementStats } from "./appStats";

// record is implemented here so we can write the below test as a direct
// analog of the one in pkg/roachpb/app_stats_test.go.  It's here rather
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

    const combined = addNumericStats(a, b, countA, countB);

    assert.approximately(combined.mean, ab.mean, 0.0000001);
    assert.approximately(combined.squared_diffs, ab.squared_diffs, 0.0000001);

    const reversed = addNumericStats(b, a, countB, countA);

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
            app: "foobar",
            distSQL: true,
            opt: true,
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
            app: "bazzer",
            distSQL: false,
            opt: false,
            failed: true,
          },
          node_id: 2,
        },
        stats: {},
      },
    ];

    const flattened = flattenStatementStats(stats);

    assert.equal(flattened.length, stats.length);

    for (let i = 0; i < flattened.length; i++) {
      assert.equal(flattened[i].statement, stats[i].key.key_data.query);
      assert.equal(flattened[i].app, stats[i].key.key_data.app);
      assert.equal(flattened[i].distSQL, stats[i].key.key_data.distSQL);
      assert.equal(flattened[i].opt, stats[i].key.key_data.opt);
      assert.equal(flattened[i].failed, stats[i].key.key_data.failed);
      assert.equal(flattened[i].node_id, stats[i].key.node_id);

      assert.equal(flattened[i].stats, stats[i].stats);
    }
  });
});

function randomInt(max: number): number {
  return Math.floor(Math.random() * max);
}

function randomFloat(scale: number): number {
  return Math.random() * scale;
}

function randomStat(scale: number = 1): NumericStat {
  return {
    mean: randomFloat(scale),
    squared_diffs: randomFloat(scale * 0.3),
  };
}

function randomStats(): StatementStatistics {
  const count = randomInt(1000);
  // tslint:disable:variable-name
  const first_attempt_count = randomInt(count);
  const max_retries = randomInt(count - first_attempt_count);
  // tslint:enable:variable-name

  return {
    count: Long.fromNumber(count),
    first_attempt_count: Long.fromNumber(first_attempt_count),
    max_retries: Long.fromNumber(max_retries),
    num_rows: randomStat(100),
    parse_lat: randomStat(),
    plan_lat: randomStat(),
    run_lat: randomStat(),
    service_lat: randomStat(),
    overhead_lat: randomStat(),
  };
}

describe("combineStatementStats", () => {
  it("combines statement statistics", () => {
    const a = randomStats();
    const b = randomStats();
    const c = randomStats();

    const ab = combineStatementStats([a, b]);
    const ac = combineStatementStats([a, c]);
    const bc = combineStatementStats([b, c]);

    // tslint:disable:variable-name
    const ab_c = combineStatementStats([ab, c]);
    const ac_b = combineStatementStats([ac, b]);
    const bc_a = combineStatementStats([bc, a]);
    // tslint:enable:variable-name

    assert.equal(ab_c.count.toString(), ac_b.count.toString());
    assert.equal(ab_c.count.toString(), bc_a.count.toString());

    assert.equal(ab_c.first_attempt_count.toString(), ac_b.first_attempt_count.toString());
    assert.equal(ab_c.first_attempt_count.toString(), bc_a.first_attempt_count.toString());

    assert.equal(ab_c.max_retries.toString(), ac_b.max_retries.toString());
    assert.equal(ab_c.max_retries.toString(), bc_a.max_retries.toString());

    assert.approximately(ab_c.num_rows.mean, ac_b.num_rows.mean, 0.0000001);
    assert.approximately(ab_c.num_rows.mean, bc_a.num_rows.mean, 0.0000001);
    assert.approximately(ab_c.num_rows.squared_diffs, ac_b.num_rows.squared_diffs, 0.0000001);
    assert.approximately(ab_c.num_rows.squared_diffs, bc_a.num_rows.squared_diffs, 0.0000001);

    assert.approximately(ab_c.parse_lat.mean, ac_b.parse_lat.mean, 0.0000001);
    assert.approximately(ab_c.parse_lat.mean, bc_a.parse_lat.mean, 0.0000001);
    assert.approximately(ab_c.parse_lat.squared_diffs, ac_b.parse_lat.squared_diffs, 0.0000001);
    assert.approximately(ab_c.parse_lat.squared_diffs, bc_a.parse_lat.squared_diffs, 0.0000001);

    assert.approximately(ab_c.plan_lat.mean, ac_b.plan_lat.mean, 0.0000001);
    assert.approximately(ab_c.plan_lat.mean, bc_a.plan_lat.mean, 0.0000001);
    assert.approximately(ab_c.plan_lat.squared_diffs, ac_b.plan_lat.squared_diffs, 0.0000001);
    assert.approximately(ab_c.plan_lat.squared_diffs, bc_a.plan_lat.squared_diffs, 0.0000001);

    assert.approximately(ab_c.run_lat.mean, ac_b.run_lat.mean, 0.0000001);
    assert.approximately(ab_c.run_lat.mean, bc_a.run_lat.mean, 0.0000001);
    assert.approximately(ab_c.run_lat.squared_diffs, ac_b.run_lat.squared_diffs, 0.0000001);
    assert.approximately(ab_c.run_lat.squared_diffs, bc_a.run_lat.squared_diffs, 0.0000001);

    assert.approximately(ab_c.service_lat.mean, ac_b.service_lat.mean, 0.0000001);
    assert.approximately(ab_c.service_lat.mean, bc_a.service_lat.mean, 0.0000001);
    assert.approximately(ab_c.service_lat.squared_diffs, ac_b.service_lat.squared_diffs, 0.0000001);
    assert.approximately(ab_c.service_lat.squared_diffs, bc_a.service_lat.squared_diffs, 0.0000001);

    assert.approximately(ab_c.overhead_lat.mean, ac_b.overhead_lat.mean, 0.0000001);
    assert.approximately(ab_c.overhead_lat.mean, bc_a.overhead_lat.mean, 0.0000001);
    assert.approximately(ab_c.overhead_lat.squared_diffs, ac_b.overhead_lat.squared_diffs, 0.0000001);
    assert.approximately(ab_c.overhead_lat.squared_diffs, bc_a.overhead_lat.squared_diffs, 0.0000001);
  });
});
