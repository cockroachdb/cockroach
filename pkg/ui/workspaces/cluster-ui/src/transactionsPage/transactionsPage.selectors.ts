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

import { adminUISelector } from "../statementsPage/statementsPage.selectors";

export const selectTransactionsSlice = createSelector(
  adminUISelector,
  adminUiState => adminUiState.transactions,
);

export const selectTransactionsData = createSelector(
  selectTransactionsSlice,
  transactionsState => transactionsState.data,
);

export const selectTransactionsLastError = createSelector(
  selectTransactionsSlice,
  state => state.lastError,
);
