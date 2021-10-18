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
} from "../statementsPage/statementsPage.selectors";

export const selectTransactionsSlice = createSelector(
  adminUISelector,
  adminUiState => adminUiState.transactions,
);

export const selectTransactionsData = createSelector(
  selectTransactionsSlice,
  transactionsState =>
    // The state is valid if we have successfully fetched data, and it has not yet been invalidated.
    transactionsState.valid ? transactionsState.data : null,
);

export const selectTransactionsLastError = createSelector(
  selectTransactionsSlice,
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
