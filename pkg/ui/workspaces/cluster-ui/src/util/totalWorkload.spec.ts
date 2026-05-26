// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { calculateTotalWorkload } from "./totalWorkload";
import { aggStatFix } from "./totalWorkload.fixture";

describe("Calculating total workload", () => {
  it("calculating total workload with one statement", () => {
    const result = calculateTotalWorkload([aggStatFix]);
    // Using approximately because float handling by javascript is imprecise
    expect(result).toBeCloseTo(48.421019, 6);
  });

  it("calculating total workload with no statements", () => {
    const result = calculateTotalWorkload([]);
    expect(result).toBe(0);
  });

  it("calculating total workload with multiple statements", () => {
    const result = calculateTotalWorkload([aggStatFix, aggStatFix, aggStatFix]);
    // Using approximately because float handling by javascript is imprecise
    expect(result).toBeCloseTo(145.263057, 6);
  });
});
