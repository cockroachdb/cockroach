// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { assert } from "chai";

import { normalizeClosedDomain } from "./utils";

describe("barCharts utils", () => {
  describe("normalizeClosedDomain", () => {
    it("returns input args if domain values are not equal", () => {
      assert.deepStrictEqual(normalizeClosedDomain([10, 15]), [10, 15]);
    });

    it("returns increased end range by 1 if input start and end values are equal", () => {
      assert.deepStrictEqual(normalizeClosedDomain([10, 10]), [10, 11]);
    });
  });
});
