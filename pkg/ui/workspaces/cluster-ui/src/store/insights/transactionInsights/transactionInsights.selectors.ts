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
import { localStorageSelector } from "src/store/utils/selectors";

export const selectTransactionInsights = (state: AppState) =>
  state.adminUI.txnInsights?.data;

export const selectTransactionInsightsError = (state: AppState) =>
  state.adminUI.txnInsights?.lastError;

export const selectSortSetting = createSelector(
  localStorageSelector,
  localStorage => localStorage["sortSetting/InsightsPage"],
);

export const selectFilters = createSelector(
  localStorageSelector,
  localStorage => localStorage["filters/InsightsPage"],
);

// Show the data as 'Loading' when the request is in flight AND the
// data is invalid or null.
export const selectTransactionInsightsLoading = (state: AppState): boolean =>
  state.adminUI.txnInsights?.inFlight &&
  (!state.adminUI.txnInsights?.valid || !state.adminUI.txnInsights?.data);
