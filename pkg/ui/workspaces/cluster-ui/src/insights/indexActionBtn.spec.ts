// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createIdxName } from "./indexActionBtn";

describe("Create index name", () => {
  const testCases = [
    {
      name: "one parameter",
      query: "CREATE INDEX ON t (i)",
      expected: "CREATE INDEX IF NOT EXISTS t_i_rec_idx ON t (i)",
    },
    {
      name: "one parameter no space",
      query: "CREATE INDEX ON t(i) STORING (k)",
      expected:
        "CREATE INDEX IF NOT EXISTS t_i_storing_rec_idx ON t(i) STORING (k)",
    },
    {
      name: "two parameters",
      query: "CREATE INDEX ON t (i, j) STORING (k)",
      expected:
        "CREATE INDEX IF NOT EXISTS t_i_j_storing_rec_idx ON t (i, j) STORING (k)",
    },
    {
      name: "one parameter, one expression",
      query: "CREATE INDEX ON t (i, (j + k)) STORING (k)",
      expected:
        "CREATE INDEX IF NOT EXISTS t_i_expr_storing_rec_idx ON t (i, (j + k)) STORING (k)",
    },
    {
      name: "one parameter, one expression no parenthesis",
      query: "CREATE INDEX ON t (i, j + k)",
      expected: "CREATE INDEX IF NOT EXISTS t_i_expr_rec_idx ON t (i, j + k)",
    },
    {
      name: "two expressions",
      query: "CREATE INDEX ON t ((i+l), (j + k)) STORING (k)",
      expected:
        "CREATE INDEX IF NOT EXISTS t_expr_expr1_storing_rec_idx ON t ((i+l), (j + k)) STORING (k)",
    },
    {
      name: "one expression, one parameter",
      query: "CREATE INDEX ON t ((i+l), j)",
      expected: "CREATE INDEX IF NOT EXISTS t_expr_j_rec_idx ON t ((i+l), j)",
    },
    {
      name: "two expressions, one parameter",
      query: "CREATE INDEX ON t ((i + l), (j + k), a) STORING (k)",
      expected:
        "CREATE INDEX IF NOT EXISTS t_expr_expr1_a_storing_rec_idx ON t ((i + l), (j + k), a) STORING (k)",
    },
    {
      name: "invalid expression, missing )",
      query: "CREATE INDEX ON t ((i + l, (j + k), a) STORING (k)",
      expected:
        "CREATE INDEX IF NOT EXISTS t_expr_expr1_expr2_storing_rec_idx ON t ((i + l, (j + k), a) STORING (k)",
    },
    {
      name: "invalid expression, extra )",
      query: "CREATE INDEX ON t ((i + l)), (j + k), a) STORING (k)",
      expected:
        "CREATE INDEX IF NOT EXISTS t_expr_storing_rec_idx ON t ((i + l)), (j + k), a) STORING (k)",
    },
    {
      name: "handles table names containing quotes, doesn't include quotes in idx name",
      query:
        'CREATE INDEX ON defaultdb.public."offers"."startdate" (n) STORING (b);',
      expected:
        'CREATE INDEX IF NOT EXISTS startdate_n_storing_rec_idx ON defaultdb.public."offers"."startdate" (n) STORING (b);',
    },
    {
      name: "handles table and column names containing quotes & whitespace, doesn't include quotes in idx name",
      query: 'CREATE INDEX ON "my table" ("my col");',
      expected:
        'CREATE INDEX IF NOT EXISTS my_table_my_col_rec_idx ON "my table" ("my col");',
    },
    {
      name: "handles quotes within quotes, doesn't include quotes in idx name",
      query: 'CREATE INDEX ON "my""table" ("with""quote");',
      expected:
        'CREATE INDEX IF NOT EXISTS mytable_withquote_rec_idx ON "my""table" ("with""quote");',
    },
  ];

  for (let i = 0; i < testCases.length; i++) {
    const test = testCases[i];
    it(test.name, () => {
      expect(createIdxName(test.query)).toEqual(test.expected);
    });
  }
});
