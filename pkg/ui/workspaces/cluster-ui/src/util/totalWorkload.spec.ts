// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { assert } from "chai";

import { calculateTotalWorkload } from "./totalWorkload";
import { aggStatFix } from "./totalWorkload.fixture";

describe("Calculating total workload", () => {
  it("calculating total workload with one statement", () => {
    const result = calculateTotalWorkload([aggStatFix]);
    // Using approximately because float handling by javascript is imprecise
    assert.approximately(result, 48.421019, 0.0000001);
  });

  it("calculating total workload with no statements", () => {
    const result = calculateTotalWorkload([]);
    assert.equal(result, 0);
  });

  it("calculating total workload with multiple statements", () => {
    const result = calculateTotalWorkload([aggStatFix, aggStatFix, aggStatFix]);
    // Using approximately because float handling by javascript is imprecise
    assert.approximately(result, 145.263057, 0.0000001);
  });
});
