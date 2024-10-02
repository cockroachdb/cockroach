// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { ComputeDurationAxisDomain } from "./domain";

describe("Domain utils", () => {
  describe("ComputeDurationAxisDomain", () => {
    it("should correctly format the label and guide", () => {
      const nsTestCases = [
        { ns: 5, value: "5.00", unit: "ns" },
        { ns: 60_000, value: "60.00", unit: "µs" },
        { ns: 7_000_000, value: "7.00", unit: "ms" },
        { ns: 40_240_000_000, value: "40.24", unit: "s" },
        { ns: 100_000_000_500, value: "1.67", unit: "min" },
        { ns: 4_000_000_000_000, value: "1.11", unit: "hr" },
        { ns: 600_000_000_000_000, value: "166.67", unit: "hr" },
      ];

      for (const { ns: extentMax, unit } of nsTestCases) {
        const axis = ComputeDurationAxisDomain([0, extentMax]);
        expect(axis).toHaveProperty("label", unit);
        expect(axis).toHaveProperty("guideFormat");
        expect(axis.guideFormat(undefined)).toEqual(`0.00 ns`);
        for (const {
          ns: guideNs,
          value: guideValue,
          unit: guideUnit,
        } of nsTestCases) {
          expect(axis.guideFormat(guideNs)).toEqual(
            `${guideValue} ${guideUnit}`,
          );
        }
      }
    });

    it("should use the units of the lowest extent if given undefined or 0", () => {
      const axis = ComputeDurationAxisDomain([2000, 2_500_000]);
      expect(axis.guideFormat(undefined)).toEqual(`0.00 µs`);
      expect(axis.guideFormat(0)).toEqual(`0.00 µs`);
    });
  });
});
