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
