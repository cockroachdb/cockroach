// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { createSelector } from "reselect";
import { localStorageSelector } from "src/store/utils/selectors";
import { AppState } from "src/store/reducers";

import { selectStatementInsightDetailsCombiner } from "src/selectors/insightsCommon.selectors";
import { selectID } from "src/selectors/common";
import { InsightEnumToLabel, StmtInsightEvent } from "src/insights";

export const selectStmtInsights = (state: AppState): StmtInsightEvent[] =>
  state.adminUI.stmtInsights?.data;

export const selectStmtInsightsError = (state: AppState): Error | null =>
  state.adminUI.stmtInsights?.lastError;

export const selectStmtInsightDetails = createSelector(
  selectStmtInsights,
  selectID,
  selectStatementInsightDetailsCombiner,
);

export const selectInsightTypes = (): string[] => {
  const insights: string[] = [];
  InsightEnumToLabel.forEach(insight => {
    insights.push(insight);
  });
  return insights;
};

export const selectColumns = createSelector(
  localStorageSelector,
  localStorage =>
    localStorage["showColumns/StatementInsightsPage"]
      ? localStorage["showColumns/StatementInsightsPage"].split(",")
      : null,
);

// Show the data as 'Loading' when the request is in flight AND the
// data is invalid or null.
export const selectStmtInsightsLoading = (state: AppState): boolean =>
  state.adminUI.stmtInsights?.inFlight &&
  (!state.adminUI.stmtInsights?.valid || !state.adminUI.stmtInsights?.data);
