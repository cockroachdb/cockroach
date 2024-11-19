// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Action } from "redux";

export const RESET_SQL_STATS = "cockroachui/sqlStats/RESET_SQL_STATS";
export const RESET_SQL_STATS_FAILED =
  "cockroachui/sqlStats/RESET_SQL_STATS_FAILED";

export function resetSQLStatsAction(): Action {
  return {
    type: RESET_SQL_STATS,
  };
}

export function resetSQLStatsFailedAction(): Action {
  return {
    type: RESET_SQL_STATS_FAILED,
  };
}
