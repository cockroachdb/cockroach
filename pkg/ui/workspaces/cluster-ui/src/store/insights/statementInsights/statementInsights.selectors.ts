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
export const selectStatementInsights = createSelector(
  (state: AppState) => state.adminUI.executionInsights?.data,
  selectFlattenedStmtInsightsCombiner,
);

export const selectStatementInsightsError = (state: AppState) =>
  state.adminUI.executionInsights?.lastError;

export const selectStatementInsightDetails = createSelector(
  selectStatementInsights,
  selectID,
  selectStatementInsightDetailsCombiner,
);

export const selectStatementInsightTypes = createSelector(
  selectStatementInsights,
  statementInsights => {
    if (!statementInsights) return [];

    const insightTypes = new Set<string>();

    statementInsights.forEach(stmtInsightEvent => {
      if (stmtInsightEvent.insights) {
        stmtInsightEvent.insights.forEach(insight => {
          if (!insightTypes.has(insight.label)) {
            insightTypes.add(insight.label);
          }
        });
      }
    });

    return Array.from(insightTypes).sort();
  },
);

export const selectColumns = createSelector(
  localStorageSelector,
  localStorage =>
    localStorage["showColumns/StatementInsightsPage"]
      ? localStorage["showColumns/StatementInsightsPage"].split(",")
      : null,
);
