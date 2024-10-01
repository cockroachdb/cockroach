// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { assert } from "chai";

import {
  BytesFitScale,
  byteUnits,
  HexStringToInt64String,
  FixFingerprintHexValue,
  EncodeUriName,
  PercentageCustom,
} from "./format";

describe("Format utils", () => {
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

  describe("HexStringToInt64String", () => {
    it("converts hex to int64", () => {
      expect(HexStringToInt64String("af6ade04cbbc1c95")).toBe(
        "12640159416348056725",
      );
      expect(HexStringToInt64String("fb9111f22f2213b7")).toBe(
        "18127289707013477303",
      );
    });
  });

  describe("FixFingerprintHexValue", () => {
    it("add leading 0 to hex values", () => {
      expect(FixFingerprintHexValue(undefined)).toBe("");
      expect(FixFingerprintHexValue(null)).toBe("");
      expect(FixFingerprintHexValue("fb9111f22f2213b7")).toBe(
        "fb9111f22f2213b7",
      );
      expect(FixFingerprintHexValue("b9111f22f2213b7")).toBe(
        "0b9111f22f2213b7",
      );
      expect(FixFingerprintHexValue("9111f22f2213b7")).toBe("009111f22f2213b7");
    });
  });

  describe("EncodeUriName", () => {
    it("decode simple string no special characters", () => {
      expect(EncodeUriName("123abc")).toBe("123abc");
    });

    it("decode string with special characters", () => {
      expect(EncodeUriName("12#_ab")).toBe("12%23_ab");
    });

    it("decode string with %", () => {
      expect(EncodeUriName("12%abc")).toBe("12%252525abc");
    });
  });

  describe("PercentageCustom", () => {
    it("percentages bigger than 1", () => {
      assert.equal(PercentageCustom(1, 1, 1), "100.0 %");
      assert.equal(PercentageCustom(0.1234, 1, 1), "12.3 %");
      assert.equal(PercentageCustom(0.23432, 1, 1), "23.4 %");
      assert.equal(PercentageCustom(0.23432, 1, 2), "23.43 %");
    });
    it("percentages between 0 and 1", () => {
      assert.equal(PercentageCustom(0, 1, 1), "0.0 %");
      assert.equal(PercentageCustom(0.00023, 1, 1), "0.02 %");
      assert.equal(PercentageCustom(0.0000023, 1, 1), "0.0002 %");
      assert.equal(PercentageCustom(0.00000000000000004, 1, 1), "~0.0 %");
    });
  });
});
