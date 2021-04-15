// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Action } from "redux";

export const RESET_SQL_STATS = "cockroachui/sqlStats/RESET_SQL_STATS";
export const RESET_SQL_STATS_COMPLETE =
  "cockroachui/sqlStats/RESET_SQL_STATS_COMPLETE";
export const RESET_SQL_STATS_FAILED =
  "cockroachui/sqlStats/RESET_SQL_STATS_FAILED";

export function resetSQLStatsAction(): Action {
  return {
    type: RESET_SQL_STATS,
  };
}

export function resetSQLStatsCompleteAction(): Action {
  return {
    type: RESET_SQL_STATS_COMPLETE,
  };
}

export function resetSQLStatsFailedAction(): Action {
  return {
    type: RESET_SQL_STATS_FAILED,
  };
}
