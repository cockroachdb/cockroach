// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";
import { createSelector } from "reselect";
import {
  api,
  defaultFilters,
  WorkloadInsightEventFilters,
  insightType,
  SchemaInsightEventFilters,
  SortSetting,
  StatementInsightEvent,
} from "@cockroachlabs/cluster-ui";
import { RouteComponentProps } from "react-router-dom";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { getMatchParamByName } from "src/util/query";

export const filtersLocalSetting = new LocalSetting<
  AdminUIState,
  WorkloadInsightEventFilters
>("filters/InsightsPage", (state: AdminUIState) => state.localSettings, {
  app: defaultFilters.app,
});

export const sortSettingLocalSetting = new LocalSetting<
  AdminUIState,
  SortSetting
>("sortSetting/InsightsPage", (state: AdminUIState) => state.localSettings, {
  ascending: false,
  columnTitle: "startTime",
});

export const selectTransactionInsights = createSelector(
  (state: AdminUIState) => state.cachedData,
  adminUiState => {
    if (!adminUiState.transactionInsights) return [];
    return adminUiState.transactionInsights.data;
  },
);

export const selectTransactionInsightDetails = createSelector(
  [
    (state: AdminUIState) => state.cachedData.transactionInsightDetails,
    (_state: AdminUIState, props: RouteComponentProps) => props,
  ],
  (
    insight,
    props,
  ): CachedDataReducerState<api.TransactionInsightEventDetailsResponse> => {
    if (!insight) {
      return null;
    }
    const insightId = getMatchParamByName(props.match, "id");
    return insight[insightId];
  },
);

export const selectStatementInsights = createSelector(
  (state: AdminUIState) => state.cachedData,
  adminUiState => {
    if (!adminUiState.statementInsights) return [];
    return adminUiState.statementInsights.data;
  },
);

export const selectStatementInsightDetails = createSelector(
  [
    (state: AdminUIState) => state.cachedData.statementInsights,
    (_state: AdminUIState, props: RouteComponentProps) => props,
  ],
  (insights, props): StatementInsightEvent => {
    if (!insights) {
      return null;
    }
    const insightId = getMatchParamByName(props.match, "id");
    return insights.data?.find(insight => insight.statementID === insightId);
  },
);

export const schemaInsightsFiltersLocalSetting = new LocalSetting<
  AdminUIState,
  SchemaInsightEventFilters
>("filters/SchemaInsightsPage", (state: AdminUIState) => state.localSettings, {
  database: defaultFilters.database,
  schemaInsightType: defaultFilters.schemaInsightType,
});

export const schemaInsightsSortLocalSetting = new LocalSetting<
  AdminUIState,
  SortSetting
>(
  "sortSetting/SchemaInsightsPage",
  (state: AdminUIState) => state.localSettings,
  {
    ascending: false,
    columnTitle: "insights",
  },
);

export const selectSchemaInsights = createSelector(
  (state: AdminUIState) => state.cachedData,
  adminUiState => {
    if (!adminUiState.schemaInsights) return [];
    return adminUiState.schemaInsights.data;
  },
);

export const selectSchemaInsightsDatabases = createSelector(
  selectSchemaInsights,
  schemaInsights => {
    if (!schemaInsights) return [];
    return Array.from(
      new Set(schemaInsights.map(schemaInsight => schemaInsight.database)),
    ).sort();
  },
);

export const selectSchemaInsightsTypes = createSelector(
  selectSchemaInsights,
  schemaInsights => {
    if (!schemaInsights) return [];
    return Array.from(
      new Set(
        schemaInsights.map(schemaInsight => insightType(schemaInsight.type)),
      ),
    ).sort();
  },
);
