// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Action } from "redux";

import { PayloadAction } from "src/interfaces/action";

export const RESET_INDEX_USAGE_STATS =
  "cockroachui/indexUsageStats/RESET_INDEX_USAGE_STATS";
export const RESET_INDEX_USAGE_STATS_COMPLETE =
  "cockroachui/indexUsageStats/RESET_INDEX_USAGE_STATS_COMPLETE";
("cockroachui/indexUsageStats/RESET_INDEX_USAGE_STATS_COMPLETE");
export const RESET_INDEX_USAGE_STATS_FAILED =
  "cockroachui/indexUsageStats/RESET_INDEX_USAGE_STATS_FAILED";

export type resetIndexUsageStatsPayload = {
  database: string;
  table: string;
};

export function resetIndexUsageStatsAction(
  database: string,
  table: string,
): PayloadAction<resetIndexUsageStatsPayload> {
  return {
    type: RESET_INDEX_USAGE_STATS,
    payload: {
      database,
      table,
    },
  };
}

export function resetIndexUsageStatsCompleteAction(): Action {
  return {
    type: RESET_INDEX_USAGE_STATS_COMPLETE,
  };
}

export function resetIndexUsageStatsFailedAction(): Action {
  return {
    type: RESET_INDEX_USAGE_STATS_FAILED,
  };
}
