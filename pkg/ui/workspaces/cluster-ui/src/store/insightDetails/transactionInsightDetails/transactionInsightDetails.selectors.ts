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
import { adminUISelector } from "src/store/utils/selectors";

const selectTransactionInsightDetailsState = createSelector(
  adminUISelector,
  adminUiState => {
    if (!adminUiState.transactionInsightDetails) return null;
    return adminUiState.transactionInsightDetails;
  },
);

export const selectTransactionInsightDetails = createSelector(
  selectTransactionInsightDetailsState,
  txnInsightDetailsState => {
    if (!txnInsightDetailsState) return null;
    return txnInsightDetailsState.data;
  },
);

export const selectTransactionInsightDetailsError = createSelector(
  selectTransactionInsightDetailsState,
  txnInsightDetailsState => {
    if (!txnInsightDetailsState) return null;
    return txnInsightDetailsState.lastError;
  },
);
