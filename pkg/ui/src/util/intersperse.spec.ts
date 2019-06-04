// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

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
