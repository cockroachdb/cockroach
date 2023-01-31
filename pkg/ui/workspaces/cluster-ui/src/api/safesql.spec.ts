// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Format, Identifier, SQL } from "./safesql";

describe("safesql", () => {
  test("format", () => {
    type customString = string;
    type customNum = number;

    const testCases: {
      expected: string;
      formatted: string;
    }[] = [
      {
        expected: `hello`,
        formatted: Format(`hello`),
      },
      {
        expected: `hello %`,
        formatted: Format(`hello %%`),
      },
      {
        expected: `hello 0`,
        formatted: Format(`hello %1`, 0),
      },
      {
        expected: `hello 'world'`,
        formatted: Format(`hello %1`, `world`),
      },
      {
        expected: `hello '''world'''`,
        formatted: Format(`hello %1`, `'world'`),
      },
      {
        expected: `hello "world"`,
        formatted: Format(`hello %1`, new Identifier(`world`)),
      },
      {
        expected: `hello """world"""`,
        formatted: Format(`hello %1`, new Identifier(`"world"`)),
      },
      {
        expected: `hello world`,
        formatted: Format(`hello %1`, new SQL(`world`)),
      },
      {
        expected: `hello "world"`,
        formatted: Format(`hello %1`, new SQL(`"world"`)),
      },
      {
        expected: `hello 'beautiful' 'world'`,
        formatted: Format(`hello %1 %2`, `beautiful`, `world`),
      },
      {
        expected: `hello 'beautiful' 'world'`,
        formatted: Format(`hello %2 %1`, `world`, `beautiful`),
      },
      {
        expected: `hello 'world'`,
        formatted: Format(`hello %1`, "world" as customString),
      },
      {
        expected: `hello 1`,
        formatted: Format(`hello %1`, 1 as customNum),
      },
    ];

    testCases.forEach(tc => {
      expect(tc.expected).toEqual(tc.formatted);
    });
  });
  test("format error", () => {});
  test("join", () => {});
});

export {};
