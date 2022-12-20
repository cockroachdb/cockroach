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
import { AppState } from "src/store/reducers";
import { selectID } from "src/selectors/common";
import { selectTxnInsightDetailsCombiner } from "src/selectors/insightsCommon.selectors";
import { TxnInsightEvent } from "src/insights";

const selectTxnContentionInsightsDetails = createSelector(
  (state: AppState) => state.adminUI.transactionInsightDetails.cachedData,
  selectID,
  (cachedTxnInsightDetails, execId) => {
    return cachedTxnInsightDetails[execId];
  },
);

const selectTxnInsightFromExecInsight = createSelector(
  (state: AppState) => state.adminUI.txnInsights?.data,
  selectID,
  (execInsights, execID): TxnInsightEvent => {
    return execInsights?.find(txn => txn.transactionExecutionID === execID);
  },
);

export const selectTransactionInsightDetails = createSelector(
  selectTxnInsightFromExecInsight,
  selectTxnContentionInsightsDetails,
  (txnInsights, txnContentionInsights) =>
    selectTxnInsightDetailsCombiner(txnInsights, txnContentionInsights?.data),
);

export const selectTransactionInsightDetailsError = createSelector(
  selectTxnContentionInsightsDetails,
  state => state?.lastError,
);
