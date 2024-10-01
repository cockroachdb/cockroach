// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { assert } from "chai";

import { intersperse } from "src/util/intersperse";

describe("intersperse", () => {
  it("puts separator in between array items", () => {
    const result = intersperse(["foo", "bar", "baz"], "-");
    assert.deepEqual(result, ["foo", "-", "bar", "-", "baz"]);
  });

  it("puts separator in between array items when given a one-item array", () => {
    const result = intersperse(["baz"], "-");
    assert.deepEqual(result, ["baz"]);
  });

  it("puts separator in between array items when given an empty array", () => {
    const result = intersperse([], "-");
    assert.deepEqual(result, []);
  });
});
