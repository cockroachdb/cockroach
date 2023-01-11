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

const selectTransactionInsightDetailsState = createSelector(
  (state: AppState) => state.adminUI.transactionInsightDetails.cachedData,
  selectID,
  (cachedTxnInsightDetails, execId) => {
    return cachedTxnInsightDetails[execId];
  },
);

export const selectTransactionInsightDetails = createSelector(
  selectTransactionInsightDetailsState,
  state => state?.data,
);

export const selectTransactionInsightDetailsError = createSelector(
  selectTransactionInsightDetailsState,
  state => state?.lastError,
);
