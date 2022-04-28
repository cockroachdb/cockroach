// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import {
  DurationFitScale,
  durationUnits,
  BytesFitScale,
  byteUnits,
} from "./format";

describe("Format utils", () => {
  describe("DurationFitScale", () => {
    it("converts nanoseconds to provided units", () => {
      // test zero values
      expect(DurationFitScale(durationUnits[0])(undefined)).toEqual("0.00 ns");
      expect(DurationFitScale(durationUnits[0])(0)).toEqual("0.00 ns");
      // "ns", "µs", "ms", "s"
      expect(DurationFitScale(durationUnits[0])(32)).toEqual("32.00 ns");
      expect(DurationFitScale(durationUnits[1])(32120)).toEqual("32.12 µs");
      expect(DurationFitScale(durationUnits[2])(32122300)).toEqual("32.12 ms");
      expect(DurationFitScale(durationUnits[3])(32122343000)).toEqual(
        "32.12 s",
      );
    });
  });

  describe("BytesFitScale", () => {
    it("converts bytes to provided units", () => {
      // test zero values
      expect(BytesFitScale(byteUnits[0])(undefined)).toEqual("0.00 B");
      expect(BytesFitScale(byteUnits[0])(0)).toEqual("0.00 B");
      // "B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"
      expect(BytesFitScale(byteUnits[0])(1)).toEqual("1.00 B");
      expect(BytesFitScale(byteUnits[1])(10240)).toEqual("10.00 KiB");
      expect(BytesFitScale(byteUnits[2])(12582912)).toEqual("12.00 MiB");
      expect(BytesFitScale(byteUnits[3])(12884901888)).toEqual("12.00 GiB");
      expect(BytesFitScale(byteUnits[4])(1.319414e13)).toEqual("12.00 TiB");
      expect(BytesFitScale(byteUnits[5])(1.3510799e16)).toEqual("12.00 PiB");
      expect(BytesFitScale(byteUnits[6])(1.3835058e19)).toEqual("12.00 EiB");
      expect(BytesFitScale(byteUnits[7])(1.4167099e22)).toEqual("12.00 ZiB");
      expect(BytesFitScale(byteUnits[8])(1.450711e25)).toEqual("12.00 YiB");
    });
  });
});
