// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { createSelector } from "reselect";

import { localStorageSelector } from "../store/utils/selectors";
import { txnStatsSelector } from "../store/transactionStats/txnStats.selector";

export const selectTransactionsData = createSelector(
  txnStatsSelector,
  transactionsState => transactionsState?.data,
);

export const selectTransactionsDataValid = createSelector(
  txnStatsSelector,
  state => state?.valid,
);

export const selectTransactionsDataInFlight = createSelector(
  txnStatsSelector,
  state => state?.inFlight,
);

export const selectTransactionsLastUpdated = createSelector(
  txnStatsSelector,
  state => state.lastUpdated,
);

export const selectTransactionsLastError = createSelector(
  txnStatsSelector,
  state => state.lastError,
);

export const selectTxnColumns = createSelector(
  localStorageSelector,
  // return array of columns if user have customized it or `null` otherwise
  localStorage =>
    localStorage["showColumns/TransactionPage"]
      ? localStorage["showColumns/TransactionPage"]?.split(",")
      : null,
);

export const selectSortSetting = createSelector(
  localStorageSelector,
  localStorage => localStorage["sortSetting/TransactionsPage"],
);

export const selectFilters = createSelector(
  localStorageSelector,
  localStorage => localStorage["filters/TransactionsPage"],
);

export const selectSearch = createSelector(
  localStorageSelector,
  localStorage => localStorage["search/TransactionsPage"],
);
