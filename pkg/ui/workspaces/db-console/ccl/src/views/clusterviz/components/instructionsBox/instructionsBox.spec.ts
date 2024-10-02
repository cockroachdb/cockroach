// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { LocalityTier } from "src/redux/localities";
import { showInstructionsBox } from "src/views/clusterviz/components/instructionsBox";

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

    cases.forEach(testCase => {
      it(`returns ${testCase.expected} for case "${testCase.desc}"`, () => {
        expect(showInstructionsBox(testCase.showMap, testCase.tiers)).toEqual(
          testCase.expected,
        );
      });
    });
  });
});
