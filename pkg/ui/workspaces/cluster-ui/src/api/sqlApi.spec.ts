// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { SqlExecutionResponse, sqlResultsAreEmpty } from "./sqlApi";

describe("sqlApi", () => {
  test("sqlResultsAreEmpty should return true when there are no rows in the response", () => {
    const testCases: {
      response: SqlExecutionResponse<unknown>;
      expected: boolean;
    }[] = [
      {
        response: {
          num_statements: 1,
          execution: {
            retries: 0,
            txn_results: [
              {
                statement: 1,
                tag: "SELECT",
                start: "start-date",
                end: "end-date",
                rows_affected: 0,
                rows: [{ hello: "world" }],
              },
            ],
          },
        },
        expected: false,
      },
      {
        response: {
          num_statements: 1,
          execution: {
            retries: 0,
            txn_results: [
              {
                statement: 1,
                tag: "SELECT",
                start: "start-date",
                end: "end-date",
                rows_affected: 0,
                rows: [],
              },
            ],
          },
        },
        expected: true,
      },
      {
        response: {
          num_statements: 1,
          execution: {
            retries: 0,
            txn_results: [
              {
                statement: 1,
                tag: "SELECT",
                start: "start-date",
                end: "end-date",
                rows_affected: 0,
                columns: [],
              },
            ],
          },
        },
        expected: true,
      },
      {
        response: {},
        expected: true,
      },
    ];

    testCases.forEach(tc => {
      expect(sqlResultsAreEmpty(tc.response)).toEqual(tc.expected);
    });
  });
});
