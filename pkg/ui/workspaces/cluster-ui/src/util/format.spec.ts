// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
      expect(BytesFitScale(byteUnits[0])(undefined)).toBe("0.00 B");
      expect(BytesFitScale(byteUnits[0])(0)).toBe("0.00 B");
      // "B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"
      expect(BytesFitScale(byteUnits[0])(1)).toBe("1.00 B");
      expect(BytesFitScale(byteUnits[1])(10240)).toBe("10.00 KiB");
      expect(BytesFitScale(byteUnits[2])(12582912)).toBe("12.00 MiB");
      expect(BytesFitScale(byteUnits[3])(12884901888)).toBe("12.00 GiB");
      expect(BytesFitScale(byteUnits[4])(1.319414e13)).toBe("12.00 TiB");
      expect(BytesFitScale(byteUnits[5])(1.3510799e16)).toBe("12.00 PiB");
      expect(BytesFitScale(byteUnits[6])(1.3835058e19)).toBe("12.00 EiB");
      expect(BytesFitScale(byteUnits[7])(1.4167099e22)).toBe("12.00 ZiB");
      expect(BytesFitScale(byteUnits[8])(1.450711e25)).toBe("12.00 YiB");
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
      expect(PercentageCustom(1, 1, 1)).toBe("100.0 %");
      expect(PercentageCustom(0.1234, 1, 1)).toBe("12.3 %");
      expect(PercentageCustom(0.23432, 1, 1)).toBe("23.4 %");
      expect(PercentageCustom(0.23432, 1, 2)).toBe("23.43 %");
    });
    it("percentages between 0 and 1", () => {
      expect(PercentageCustom(0, 1, 1)).toBe("0.0 %");
      expect(PercentageCustom(0.00023, 1, 1)).toBe("0.02 %");
      expect(PercentageCustom(0.0000023, 1, 1)).toBe("0.0002 %");
      expect(PercentageCustom(0.00000000000000004, 1, 1)).toBe("~0.0 %");
    });
  });
});
