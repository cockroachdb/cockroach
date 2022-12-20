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
  defaultFilters,
  WorkloadInsightEventFilters,
  insightType,
  SchemaInsightEventFilters,
  SortSetting,
  selectFlattenedStmtInsightsCombiner,
  selectID,
  selectStatementInsightDetailsCombiner,
  selectTxnInsightsCombiner,
  TxnContentionInsightDetails,
  selectTxnInsightDetailsCombiner,
  Insight,
  FlattenedStmtInsightEvent,
} from "@cockroachlabs/cluster-ui";

export const filtersLocalSetting = new LocalSetting<
  AdminUIState,
  WorkloadInsightEventFilters
>("filters/InsightsPage", (state: AdminUIState) => state.localSettings, {
  app: defaultFilters.app,
  workloadInsightType: defaultFilters.workloadInsightType,
});

export const sortSettingLocalSetting = new LocalSetting<
  AdminUIState,
  SortSetting
>("sortSetting/InsightsPage", (state: AdminUIState) => state.localSettings, {
  ascending: false,
  columnTitle: "startTime",
});

export const selectTransactionInsights = createSelector(
  (state: AdminUIState) => state.cachedData.executionInsights?.data,
  (state: AdminUIState) => state.cachedData.transactionInsights?.data,
  selectTxnInsightsCombiner,
);

export const selectTransactionInsightTypes = createSelector(
  selectTransactionInsights,
  transactionInsights => {
    if (!transactionInsights) return [];

    const transactionInsightTypes = new Set<string>();
    transactionInsights.forEach(transactionInsightEvent => {
      if (transactionInsightEvent.insights) {
        transactionInsightEvent.insights.forEach((insight: Insight) => {
          if (!transactionInsightTypes.has(insight.label)) {
            transactionInsightTypes.add(insight.label);
          }
        });
      }
    });

    return Array.from(transactionInsightTypes).sort();
  },
);

const selectTxnContentionInsightDetails = createSelector(
  [
    (state: AdminUIState) => state.cachedData.transactionInsightDetails,
    selectID,
  ],
  (insight, insightId): TxnContentionInsightDetails => {
    if (!insight) {
      return null;
    }
    return insight[insightId]?.data;
  },
);

const selectTxnInsightFromExecInsight = createSelector(
  (state: AdminUIState) => state.cachedData.executionInsights?.data,
  selectID,
  (execInsights, execID) => {
    return execInsights?.find(txn => txn.transactionExecutionID === execID);
  },
);

export const selectTxnInsightDetails = createSelector(
  selectTxnInsightFromExecInsight,
  selectTxnContentionInsightDetails,
  selectTxnInsightDetailsCombiner,
);

export const selectTransactionInsightDetailsError = createSelector(
  (state: AdminUIState) => state.cachedData.transactionInsightDetails,
  selectID,
  (insight, insightId): Error | null => {
    if (!insight) {
      return null;
    }
    return insight[insightId]?.lastError;
  },
);

export const selectStatementInsights = createSelector(
  (state: AdminUIState) => state.cachedData.executionInsights?.data,
  selectFlattenedStmtInsightsCombiner,
);

export const selectStatementInsightTypes = createSelector(
  selectStatementInsights,
  statementInsights => {
    if (!statementInsights) return [];
    const insightTypes = new Set<string>();

    statementInsights.forEach((stmtInsightEvent: FlattenedStmtInsightEvent) => {
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

export const selectStatementInsightDetails = createSelector(
  selectStatementInsights,
  selectID,
  selectStatementInsightDetailsCombiner,
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
