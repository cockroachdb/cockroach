// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
