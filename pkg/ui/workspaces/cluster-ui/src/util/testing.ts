// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { SqlExecutionResponse } from "../api";

/**
 * CollapseWhitespace collapses all adjacent whitespace to a single space
 * character and trims leading and trailing whitespace. It is useful when
 * normalizing text broken up by HTML elements for test assertions.
 *
 * @param s input string to collapse
 */
export function CollapseWhitespace(s: string): string {
  return s.replace(/\s\s+/g, " ").trim();
}

/**
 * MockSqlResponse formulates a mock response from a call to the
 * executeInternalSql function.
 *
 * @param rows rows returned by the query
 */
export function MockSqlResponse<T>(rows: T[]): SqlExecutionResponse<T> {
  return {
    execution: {
      retries: 0,
      txn_results: [
        {
          tag: "",
          start: "",
          end: "",
          rows_affected: 0,
          statement: 1,
          rows: [...rows],
        },
      ],
    },
  };
}
