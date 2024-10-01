// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createSelector } from "reselect";

import { TxnInsightEvent } from "src/insights";
import { selectID, selectStmtInsights } from "src/selectors/common";
import { selectTxnInsightDetailsCombiner } from "src/selectors/insightsCommon.selectors";
import { AppState } from "src/store/reducers";

const selectTxnContentionInsightsDetails = createSelector(
  (state: AppState) => state.adminUI?.txnInsightDetails.cachedData,
  selectID,
  (cachedTxnInsightDetails, execId) => {
    return cachedTxnInsightDetails[execId];
  },
);

const selectTxnInsightFromExecInsight = createSelector(
  (state: AppState) => state.adminUI?.txnInsights?.data?.results,
  selectID,
  (execInsights, execID): TxnInsightEvent => {
    return execInsights?.find(txn => txn.transactionExecutionID === execID);
  },
);

export const selectTransactionInsightDetails = createSelector(
  selectTxnInsightFromExecInsight,
  selectTxnContentionInsightsDetails,
  selectStmtInsights,
  (txnInsights, txnContentionInsights, stmtInsights) =>
    selectTxnInsightDetailsCombiner(
      txnInsights,
      txnContentionInsights?.data,
      stmtInsights,
    ),
);

export const selectTransactionInsightDetailsError = createSelector(
  selectTxnContentionInsightsDetails,
  state => state?.errors,
);

export const selectTransactionInsightDetailsMaxSizeReached = createSelector(
  selectTxnContentionInsightsDetails,
  state => state?.maxSizeReached,
);
