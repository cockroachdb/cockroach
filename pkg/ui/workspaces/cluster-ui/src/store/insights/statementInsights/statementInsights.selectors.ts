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

import {
  selectFlattenedStmtInsightsCombiner,
  selectStatementInsightDetailsCombiner,
} from "src/selectors/insightsCommon.selectors";
import { selectID } from "src/selectors/common";
import { InsightEnumToLabel } from "src/insights";

export const selectExecutionInsights = createSelector(
  (state: AppState) => state.adminUI.executionInsights?.data,
  selectFlattenedStmtInsightsCombiner,
);

export const selectExecutionInsightsError = (state: AppState) =>
  state.adminUI.executionInsights?.lastError;

export const selectStatementInsightDetails = createSelector(
  selectExecutionInsights,
  selectID,
  selectStatementInsightDetailsCombiner,
);

export const selectInsightTypes = () => {
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

export const selectExecutionInsightsLoading = (state: AppState) =>
  !state.adminUI.executionInsights?.valid ||
  state.adminUI.executionInsights?.inFlight;
