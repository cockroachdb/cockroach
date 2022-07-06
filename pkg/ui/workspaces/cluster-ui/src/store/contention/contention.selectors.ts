// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { createSelector } from "reselect";
import { adminUISelector } from "../utils/selectors";
import { getInsightTransactionsFromContentionEvent } from "src/insights/utils";

export const selectContentionTransactionEvents = createSelector(
  adminUISelector,
  adminUiState => {
    if (!adminUiState.contentionTransactions) return [];
    return getInsightTransactionsFromContentionEvent(
      adminUiState.contentionTransactions.data,
    );
  },
);

export const selectStatementsFromContentionEvent = createSelector(
  adminUISelector,
  adminUIState => {
    return adminUIState.sqlStats.data.statements.filter(statement =>
      adminUIState.contentionTransactions.data.events
        .map(event => event.blocking_txn_fingerprint_id)
        .includes(statement.key.key_data.transaction_fingerprint_id),
    );
  },
);
