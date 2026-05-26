// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { LARGE_RESULT_SIZE, LONG_TIMEOUT, SqlExecutionRequest } from "./sqlApi";

export const makeInsightsSqlRequest = (
  queries: Array<string | null>,
): SqlExecutionRequest => ({
  statements: queries.filter(q => q).map(query => ({ sql: query })),
  execute: true,
  max_result_size: LARGE_RESULT_SIZE,
  timeout: LONG_TIMEOUT,
});
