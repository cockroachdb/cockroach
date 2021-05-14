// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { ComputeByteAxisDomain } from "oss/src/views/cluster/util/graphs";
import assert from "assert";

describe("ComputeByteAxisDomain", () => {
  it("should compute an IEC-based domain", () => {
    const domain = ComputeByteAxisDomain([0, 1000000]);
    assert.deepEqual(domain.ticks, [256000, 512000, 768000]);
    assert.equal(domain.tickFormat(256000), "250");
    assert.equal(domain.guideFormat(256000), "250.00 KiB");
    assert.equal(domain.label, "KiB");
  });
  // This test is here to demonstrate some of the issues that arise
  // when reusing a tick formatter with different ranges of data than
  // it was created with
  it("will produce odd data when extent changes but same tickformat is used", () => {
    const originalDomain = ComputeByteAxisDomain([0, 1000000]);
    assert.deepEqual(originalDomain.ticks, [256000, 512000, 768000]);
    assert.equal(originalDomain.tickFormat(256000), "250");
    assert.equal(originalDomain.guideFormat(256000), "250.00 KiB");
    assert.equal(originalDomain.label, "KiB");
    const newDomain = ComputeByteAxisDomain([0, 1000000000]);
    assert.deepEqual(newDomain.ticks, [262144000, 524288000, 786432000]);
    assert.equal(newDomain.tickFormat(262144000), "250");
    assert.equal(newDomain.guideFormat(262144000), "250.00 MiB");
    assert.equal(newDomain.label, "MiB");
    // Now trying new domain with old ticks
    // The `m` in this case is "milli" meaning `10^-3`
    // see: https://github.com/d3/d3-format#locale_formatPrefix
    assert.equal(newDomain.tickFormat(256000), "244.140625m");
    assert.equal(newDomain.guideFormat(256000), "0.24 MiB");
  });
});
