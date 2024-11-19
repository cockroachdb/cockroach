// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { assert } from "chai";

import {
  greaterOrEqualThanVersion,
  parseStringToVersion,
  SemVersion,
} from "./versions";

// cribbed test from managed-service
describe("greaterOrEqualThanVersion", () => {
  // [input, compare to, expected, description]
  const data: Array<[string, SemVersion, boolean, string]> = [
    [
      "1000019.1.0-beta.20190225",
      [19, 1, NaN],
      false,
      "specific prerelease version is less than 'latest' version",
    ],
    ["1000019.1.0-beta.20190225", [19, 1, 0], true, "versions are equal"],
    [
      "1000021.1-build",
      [20, 1, NaN],
      true,
      "latest 21.1 ver is greater than latest 20.1 ver",
    ],
    [
      "1000021.1.2",
      [20, 1, 3],
      true,
      "21.1.2 ver is greater than latest 20.1.3 ver",
    ],
  ];
  data.forEach(([input, otherVersion, expected, name]) => {
    it(name, () => {
      expect(greaterOrEqualThanVersion(input, otherVersion)).toEqual(expected);
    });
  });
});

describe("Parsing version strings", () => {
  it("parse with patch version and build version", () => {
    const result = parseStringToVersion("1000022.2.5-5");
    assert.deepStrictEqual(result, [22, 2, 5]);
  });

  it("parse with patch version, no build version", () => {
    const result = parseStringToVersion("1000022.2.5");
    assert.deepStrictEqual(result, [22, 2, 5]);
  });

  it("parse with no patch version, with build version", () => {
    const result = parseStringToVersion("1000023.1-8");
    assert.deepStrictEqual(result, [23, 1, 0]);
  });

  it("parse with no patch version, with long build version", () => {
    const result = parseStringToVersion("1000022.2-100");
    assert.deepStrictEqual(result, [22, 2, 0]);
  });

  it("parse with no patch version, with build version as characters", () => {
    const result = parseStringToVersion("1000023.1-build");
    assert.deepStrictEqual(result, [23, 1, 0]);
  });

  it("parse with patch version, large build version", () => {
    const result = parseStringToVersion("1000019.1.0-beta.20190225");
    assert.deepStrictEqual(result, [19, 1, 0]);
  });

  it("bad input no split", () => {
    const result = parseStringToVersion("test");
    assert.deepStrictEqual(result, [0, 0, 0]);
  });

  it("bad input no match", () => {
    const result = parseStringToVersion("10000wontmatch");
    assert.deepStrictEqual(result, [0, 0, 0]);
  });

  it("non-strip input", () => {
    const result = parseStringToVersion("22.2-100");
    assert.deepStrictEqual(result, [22, 2, 0]);
  });
});
