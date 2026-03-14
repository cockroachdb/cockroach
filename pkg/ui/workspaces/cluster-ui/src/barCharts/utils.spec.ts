// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { normalizeClosedDomain } from "./utils";

describe("barCharts utils", () => {
  describe("normalizeClosedDomain", () => {
    it("returns input args if domain values are not equal", () => {
      expect(normalizeClosedDomain([10, 15])).toEqual([10, 15]);
    });

    it("returns increased end range by 1 if input start and end values are equal", () => {
      expect(normalizeClosedDomain([10, 10])).toEqual([10, 11]);
    });
  });
});
