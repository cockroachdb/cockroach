// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  SqlExecutionResponse,
  sqlResultsAreEmpty,
  isUpgradeError,
} from "./sqlApi";

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

  test("isUpgradeError", () => {
    const tests = [
      {
        msg: 'relation "crdb_internal.txn_execution_insights" does not exist',
        expected: true,
      },
      {
        msg: 'extra text____relation "my table that doesnt exist" does not exist++++ extra text',
        expected: true,
      },
      {
        msg: 'column "hello" does not exist',
        expected: true,
      },
      {
        msg: 'blah blah blah column "my_new_column" does not exist obs-fun-times',
        expected: true,
      },
      {
        msg: "not an upgrade error",
        expected: false,
      },
    ];
    tests.forEach(tc => {
      expect(isUpgradeError(tc.msg)).toEqual(tc.expected);
    });
  });
});
