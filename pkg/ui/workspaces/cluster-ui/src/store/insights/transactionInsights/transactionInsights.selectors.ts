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

const selectTransactionInsightsState = createSelector(
  adminUISelector,
  adminUiState => {
    if (!adminUiState.transactionInsights) return null;
    return adminUiState.transactionInsights;
  },
);

export const selectTransactionInsights = createSelector(
  selectTransactionInsightsState,
  txnInsightsState => {
    if (!txnInsightsState) return [];
    return txnInsightsState.data;
  },
);

export const selectTransactionInsightsError = createSelector(
  selectTransactionInsightsState,
  txnInsightsState => {
    if (!txnInsightsState) return null;
    return txnInsightsState.lastError;
  },
);

export const selectSortSetting = createSelector(
  localStorageSelector,
  localStorage => localStorage["sortSetting/InsightsPage"],
);

export const selectFilters = createSelector(
  localStorageSelector,
  localStorage => localStorage["filters/InsightsPage"],
);
