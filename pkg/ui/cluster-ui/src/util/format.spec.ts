// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { assert } from "chai";
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
      assert.equal(DurationFitScale(durationUnits[0])(undefined), "0.00 ns");
      assert.equal(DurationFitScale(durationUnits[0])(0), "0.00 ns");
      // "ns", "µs", "ms", "s"
      assert.equal(DurationFitScale(durationUnits[0])(32), "32.00 ns");
      assert.equal(DurationFitScale(durationUnits[1])(32120), "32.12 µs");
      assert.equal(DurationFitScale(durationUnits[2])(32122300), "32.12 ms");
      assert.equal(DurationFitScale(durationUnits[3])(32122343000), "32.12 s");
    });
  });

  describe("BytesFitScale", () => {
    it("converts bytes to provided units", () => {
      // test zero values
      assert.equal(BytesFitScale(byteUnits[0])(undefined), "0.00 B");
      assert.equal(BytesFitScale(byteUnits[0])(0), "0.00 B");
      // "B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"
      assert.equal(BytesFitScale(byteUnits[0])(1), "1.00 B");
      assert.equal(BytesFitScale(byteUnits[1])(10240), "10.00 KiB");
      assert.equal(BytesFitScale(byteUnits[2])(12582912), "12.00 MiB");
      assert.equal(BytesFitScale(byteUnits[3])(12884901888), "12.00 GiB");
      assert.equal(BytesFitScale(byteUnits[4])(1.319414e13), "12.00 TiB");
      assert.equal(BytesFitScale(byteUnits[5])(1.3510799e16), "12.00 PiB");
      assert.equal(BytesFitScale(byteUnits[6])(1.3835058e19), "12.00 EiB");
      assert.equal(BytesFitScale(byteUnits[7])(1.4167099e22), "12.00 ZiB");
      assert.equal(BytesFitScale(byteUnits[8])(1.450711e25), "12.00 YiB");
    });
  });
});
