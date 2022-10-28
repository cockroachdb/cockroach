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
import {
  adminUISelector,
  localStorageSelector,
} from "src/store/utils/selectors";
import { getMatchParamByName } from "../../../util";
import { RouteComponentProps } from "react-router";
import { StatementInsightEvent } from "../../../insights";
import { AppState } from "../../reducers";

const selectStatementInsightsState = createSelector(
  adminUISelector,
  adminUiState => {
    if (!adminUiState.statementInsights) return null;
    return adminUiState.statementInsights;
  },
);

export const selectStatementInsights = createSelector(
  selectStatementInsightsState,
  stmtInsightsState => {
    if (!stmtInsightsState) return [];
    return stmtInsightsState.data;
  },
);

export const selectStatementInsightsError = createSelector(
  selectStatementInsightsState,
  stmtInsightsState => {
    if (!stmtInsightsState) return null;
    return stmtInsightsState.lastError;
  },
);

export const selectStatementInsightDetails = createSelector(
  selectStatementInsightsState,
  (_state: AppState, props: RouteComponentProps) => props,
  (statementInsights, props): StatementInsightEvent => {
    if (!statementInsights) return null;

    const insightId = getMatchParamByName(props.match, "id");
    return statementInsights.data?.find(
      statementInsight => statementInsight.statementID === insightId,
    );
  },
);

export const selectColumns = createSelector(
  localStorageSelector,
  localStorage =>
    localStorage["showColumns/StatementInsightsPage"]
      ? localStorage["showColumns/StatementInsightsPage"].split(",")
      : null,
);
