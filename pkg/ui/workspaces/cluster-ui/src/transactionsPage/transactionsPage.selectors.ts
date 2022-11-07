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

import {
  adminUISelector,
  localStorageSelector,
} from "../store/utils/selectors";
import { sqlStatsSelector } from "../store/sqlStats/sqlStats.selector";

export const selectTransactionsData = createSelector(
  sqlStatsSelector,
  transactionsState =>
    // The state is valid if we have successfully fetched data, and it has not yet been invalidated.
    transactionsState.valid ? transactionsState.data : null,
);

export const selectTransactionsLastError = createSelector(
  sqlStatsSelector,
  state => state.lastError,
);

export const selectTxnColumns = createSelector(
  localStorageSelector,
  // return array of columns if user have customized it or `null` otherwise
  localStorage =>
    localStorage["showColumns/TransactionPage"]
      ? localStorage["showColumns/TransactionPage"].split(",")
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

export const selectTransactionInsightCounts = createSelector(
  adminUISelector,
  state => {
    return state?.transactionInsightCounts?.data;
  },
);
