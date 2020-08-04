// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt
import { assert } from "chai";

import { showInstructionsBox } from "src/views/clusterviz/components/instructionsBox";
import { LocalityTier } from "src/redux/localities";

describe("InstructionsBox component", () => {
  describe("showInstructionsBox", () => {
    interface TestCase {
      showMap: boolean;
      tiers: LocalityTier[];
      expected: boolean;
      desc: string;
    }

    const cases: TestCase[] = [
      {
        desc: "showing map, at top level",
        showMap: true,
        tiers: [],
        expected: false,
      },
      {
        desc: "showing map, down a level",
        showMap: true,
        tiers: [{ key: "foo", value: "bar" }],
        expected: false,
      },
      {
        desc: "not showing map, at top level",
        showMap: false,
        tiers: [],
        expected: true,
      },
      {
        desc: "not showing map, down a level",
        showMap: false,
        tiers: [{ key: "foo", value: "bar" }],
        expected: false,
      },
    ];

    cases.forEach((testCase) => {
      it(`returns ${testCase.expected} for case "${testCase.desc}"`, () => {
        assert.equal(
          showInstructionsBox(testCase.showMap, testCase.tiers),
          testCase.expected,
        );
      });
    });
  });
});
