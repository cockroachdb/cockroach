import { assert } from "chai";

import { addNumericStats, NumericStat } from "./appStats";

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
