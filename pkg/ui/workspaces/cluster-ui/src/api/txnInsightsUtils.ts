// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { LARGE_RESULT_SIZE, LONG_TIMEOUT, SqlExecutionRequest } from "./sqlApi";

export const makeInsightsSqlRequest = (
  queries: Array<string | null>,
): SqlExecutionRequest => ({
  statements: queries.filter(q => q).map(query => ({ sql: query })),
  execute: true,
  max_result_size: LARGE_RESULT_SIZE,
  timeout: LONG_TIMEOUT,
});
