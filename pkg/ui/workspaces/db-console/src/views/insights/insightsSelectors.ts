// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  defaultFilters,
  WorkloadInsightEventFilters,
  insightType,
  SchemaInsightEventFilters,
  SortSetting,
  selectID,
  selectTransactionFingerprintID,
  selectStatementInsightDetailsCombiner,
  selectTxnInsightDetailsCombiner,
  InsightEnumToLabel,
  TxnInsightDetails,
  api,
  util,
} from "@cockroachlabs/cluster-ui";
import { createSelector } from "reselect";

import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";

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

export const selectTransactionInsightsLoading = (state: AdminUIState) =>
  state.cachedData.txnInsights?.inFlight &&
  (!state.cachedData.txnInsights?.valid ||
    !state.cachedData.txnInsights?.data?.results);

export const selectTransactionInsights = (state: AdminUIState) =>
  state.cachedData.txnInsights?.valid
    ? state.cachedData.txnInsights?.data?.results
    : null;

const selectCachedTxnInsightDetails = createSelector(
  [(state: AdminUIState) => state.cachedData.txnInsightDetails, selectID],
  (insight, insightId): TxnInsightDetails => {
    if (!insight) {
      return null;
    }
    return insight[insightId]?.data?.results.result;
  },
);

const selectTxnInsight = createSelector(
  (state: AdminUIState) => state.cachedData.txnInsights?.data?.results,
  selectID,
  (insights, execID) => {
    return insights?.find(txn => txn.transactionExecutionID === execID);
  },
);

export const selectTxnInsightsMaxApiReached = (
  state: AdminUIState,
): boolean => {
  return !!state.cachedData.txnInsights?.data?.maxSizeReached;
};

export const selectStmtInsights = (state: AdminUIState) => {
  return state.cachedData.stmtInsights?.data?.results;
};

export const selectStmtInsightsMaxApiReached = (
  state: AdminUIState,
): boolean => {
  return !!state.cachedData.stmtInsights?.data?.maxSizeReached;
};

export const selectTxnInsightDetails = createSelector(
  selectTxnInsight,
  selectCachedTxnInsightDetails,
  selectStmtInsights,
  selectTxnInsightDetailsCombiner,
);

export const selectTransactionInsightDetailsError = createSelector(
  (state: AdminUIState) => state.cachedData.txnInsightDetails,
  selectID,
  (insights, insightId: string): api.TxnInsightDetailsReqErrs | null => {
    if (!insights) {
      return null;
    }
    // TODO (koorosh): code within IF clause below doesn't look like affect result of the function and
    // can be removed.
    const reqErrors = insights[insightId]?.data?.results.errors;
    if (insights[insightId]?.lastError) {
      Object.keys(reqErrors).forEach(
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-ignore
        (key: keyof api.TxnInsightDetailsReqErrs) => {
          reqErrors[key] = insights[insightId].lastError;
        },
      );
    }

    return insights[insightId]?.data?.results.errors;
  },
);

export const selectTransactionInsightDetailsMaxSizeReached = createSelector(
  (state: AdminUIState) => state.cachedData.txnInsightDetails,
  selectID,
  (insights, insightId: string): boolean =>
    insights[insightId]?.data?.maxSizeReached,
);

// Data is showed as loading when the request is in flight AND we have
// no data, (i.e. first request), or the current loaded data is
// invalid (e.g. time range changed)
export const selectStmtInsightsLoading = (state: AdminUIState) =>
  state.cachedData.stmtInsights?.inFlight &&
  (!state.cachedData.stmtInsights?.data ||
    !state.cachedData.stmtInsights.valid);

export const selectTxnInsightsByFingerprint = createSelector(
  selectTransactionInsights,
  selectTransactionFingerprintID,
  (execInsights, fingerprintID) => {
    if (fingerprintID == null) {
      return null;
    }
    const id = util.FixFingerprintHexValue(BigInt(fingerprintID).toString(16));
    return execInsights?.filter(txn => txn.transactionFingerprintID === id);
  },
);

export const selectInsightTypes = () => {
  const insights: string[] = [];
  InsightEnumToLabel.forEach(insight => {
    insights.push(insight);
  });
  return insights;
};

export const selectStatementInsightDetails = createSelector(
  selectStmtInsights,
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
    if (!adminUiState?.schemaInsights) return [];
    return adminUiState?.schemaInsights.data?.results;
  },
);

export const selectSchemaInsightsMaxApiReached = (
  state: AdminUIState,
): boolean => {
  return !!state.cachedData.schemaInsights?.data?.maxSizeReached;
};

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
