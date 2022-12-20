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
import { selectTxnInsightsCombiner } from "src/selectors/insightsCommon.selectors";
import { localStorageSelector } from "src/store/utils/selectors";

const selectTransactionInsightsData = (state: AppState) =>
  state.adminUI.txnInsights?.data;

export const selectTransactionInsights = createSelector(
  selectTransactionInsightsData,
  selectTxnInsightsCombiner,
);

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

export const selectTransactionInsightsLoading = (state: AppState) =>
  !state.adminUI.txnInsights?.valid || state.adminUI.txnInsights?.inFlight;
