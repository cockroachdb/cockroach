// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  Format,
  Identifier,
  Join,
  QuoteIdentifier,
  SQL,
  SqlFormatArg,
} from "./safesql";

describe("safesql", () => {
  test("format", () => {
    type CustomString = string;
    type CustomNum = number;

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
        formatted: Format(`hello %1`, [0]),
      },
      {
        expected: `hello 'world'`,
        formatted: Format(`hello %1`, [`world`]),
      },
      {
        expected: `hello '''world'''`,
        formatted: Format(`hello %1`, [`'world'`]),
      },
      {
        expected: `hello "world"`,
        formatted: Format(`hello %1`, [new Identifier(`world`)]),
      },
      {
        expected: `hello """world"""`,
        formatted: Format(`hello %1`, [new Identifier(`"world"`)]),
      },
      {
        expected: `hello world`,
        formatted: Format(`hello %1`, [new SQL(`world`)]),
      },
      {
        expected: `hello "world"`,
        formatted: Format(`hello %1`, [new SQL(`"world"`)]),
      },
      {
        expected: `hello 'beautiful' 'world'`,
        formatted: Format(`hello %1 %2`, [`beautiful`, `world`]),
      },
      {
        expected: `hello 'beautiful' 'world'`,
        formatted: Format(`hello %2 %1`, [`world`, `beautiful`]),
      },
      {
        expected: `hello 'world'`,
        formatted: Format(`hello %1`, ["world" as CustomString]),
      },
      {
        expected: `hello 1`,
        formatted: Format(`hello %1`, [1 as CustomNum]),
      },
    ];

    testCases.forEach(tc => {
      expect(tc.formatted).toEqual(tc.expected);
    });
  });

  test("format error", () => {
    const testCases: {
      expected: string;
      format: string;
      args: SqlFormatArg[];
    }[] = [
      { format: `hello %s`, args: null, expected: `invalid placeholder: %s` },
      { format: `hello %`, args: null, expected: `invalid placeholder: %` },
      { format: `hello %1`, args: null, expected: `bad placeholder index: %1` },
      {
        format: `hello%1`,
        args: null,
        expected: `invalid separator: 'o' is not punctuation or whitespace`,
      },
      {
        format: `hello %1`,
        args: [new Date() as any],
        expected: `bad argument 1: unsupported type: object`,
      },
    ];

    testCases.forEach(tc => {
      expect(() => {
        Format(tc.format, tc.args);
      }).toThrow(tc.expected);
    });
  });

  test("join", () => {
    const testCases: {
      expected: string;
      got: SQL;
    }[] = [
      {
        expected: `1, 2, 3`,
        got: Join([1, 2, 3], new SQL(", ")),
      },
      {
        expected: `'a', 'b', 'c'`,
        got: Join(["a", "b", "c"], new SQL(", ")),
      },
      {
        expected: `'one item'`,
        got: Join(["one item"], new SQL(", ")),
      },
      {
        expected: `"IDENT_A" "IDENT_B" "IDENT_C"`,
        got: Join(
          [
            new Identifier("IDENT_A"),
            new Identifier("IDENT_B"),
            new Identifier("IDENT_C"),
          ],
          new SQL(" "),
        ),
      },
      {
        expected: `1 'mixed' "ident"`,
        got: Join([1, "mixed", new Identifier("ident")], new SQL(" ")),
      },
    ];

    testCases.forEach(tc => {
      expect(tc.got.sqlString()).toEqual(tc.expected);
    });
  });

  // https://www.cockroachlabs.com/docs/stable/keywords-and-identifiers#rules-for-identifiers
  // https://www.postgresql.org/docs/15/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
  test("QuoteIdentifier", () => {
    const testCases = [
      ["foobar", '"foobar"'],
      ['weird"table', '"weird""table"'],
      ['"quoted"', '"""quoted"""'],

      // Anti-test cases: The following cases document the inputs that
      // QuoteIdentifier can not deal with.
      // 1. A fully qualified name that is not comprised of quoted identifiers.
      // 2. A fully qualified name that is already made of quoted identifiers.
      ["schema.table", '"schema.table"'], // Invalid SQL output.
      ['"schema"."table"', '"""schema"".""table"""'], // Invalid SQL output.
    ];

    for (const tcase of testCases) {
      const res = QuoteIdentifier(tcase[0]);
      const expected = tcase[1];
      expect(res).toEqual(expected);
    }
  });
});
