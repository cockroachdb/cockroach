// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { ComputeByteAxisDomain } from "@cockroachlabs/cluster-ui";

describe("ComputeByteAxisDomain", () => {
  it("should compute an IEC-based domain", () => {
    const domain = ComputeByteAxisDomain([0, 1000000]);
    expect(domain.ticks).toEqual([256000, 512000, 768000]);
    expect(domain.tickFormat(256000)).toBe("250");
    expect(domain.guideFormat(256000)).toBe("250.00 KiB");
    expect(domain.label).toBe("KiB");
  });
  // This test is here to demonstrate some of the issues that arise
  // when reusing a tick formatter with different ranges of data than
  // it was created with
  it("will produce odd data when extent changes but same tickformat is used", () => {
    const originalDomain = ComputeByteAxisDomain([0, 1000000]);
    expect(originalDomain.ticks).toEqual([256000, 512000, 768000]);
    expect(originalDomain.tickFormat(256000)).toBe("250");
    expect(originalDomain.guideFormat(256000)).toBe("250.00 KiB");
    expect(originalDomain.label).toBe("KiB");

    const newDomain = ComputeByteAxisDomain([0, 1000000000]);
    expect(newDomain.ticks).toEqual([262144000, 524288000, 786432000]);
    expect(newDomain.tickFormat(262144000)).toBe("250");
    expect(newDomain.guideFormat(262144000)).toBe("250.00 MiB");
    expect(newDomain.label).toBe("MiB");
    // Now trying new domain with old ticks
    // The `m` in this case is "milli" meaning `10^-3`
    // see: https://github.com/d3/d3-format#locale_formatPrefix
    expect(newDomain.tickFormat(256000)).toBe("0.2441");
    expect(newDomain.guideFormat(256000)).toBe("0.24 MiB");
  });
});
