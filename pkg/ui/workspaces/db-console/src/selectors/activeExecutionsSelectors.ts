// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  ActiveExecutions,
  selectActiveExecutionsCombiner,
  getActiveStatement,
  getActiveTransaction,
  getContentionDetailsFromLocksAndTxns,
  selectExecutionID,
} from "@cockroachlabs/cluster-ui";
import { createSelector } from "reselect";

import { CachedDataReducerState } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { SessionsResponseMessage } from "src/util/api";

const selectSessions = (state: AdminUIState) => state.cachedData.sessions?.data;

const selectClusterLocks = (state: AdminUIState) =>
  state.cachedData.clusterLocks?.data?.results;

export const selectClusterLocksMaxApiSizeReached = (
  state: AdminUIState,
): boolean => {
  return !!state.cachedData.clusterLocks?.data?.maxSizeReached;
};

export const selectActiveExecutions = createSelector(
  selectSessions,
  selectClusterLocks,
  selectActiveExecutionsCombiner,
);

export const selectActiveStatements = createSelector(
  selectActiveExecutions,
  (executions: ActiveExecutions) => executions.statements,
);

export const selectActiveStatement = createSelector(
  selectActiveStatements,
  selectExecutionID,
  getActiveStatement,
);

export const selectAppName = createSelector(
  (state: AdminUIState) => state.cachedData.sessions,
  (state?: CachedDataReducerState<SessionsResponseMessage>) => {
    if (!state?.data) {
      return null;
    }
    return state.data.internal_app_name_prefix;
  },
);

export const selectActiveTransactions = createSelector(
  selectActiveExecutions,
  (executions: ActiveExecutions) => executions.transactions,
);

export const selectActiveTransaction = createSelector(
  selectActiveTransactions,
  selectExecutionID,
  getActiveTransaction,
);

export const selectContentionDetailsForTransaction = createSelector(
  selectClusterLocks,
  selectActiveTransactions,
  selectActiveTransaction,
  getContentionDetailsFromLocksAndTxns,
);

const selectActiveTxnFromStmt = createSelector(
  selectActiveStatement,
  selectActiveTransactions,
  (stmt, transactions) => {
    return stmt
      ? transactions.find(txn => txn.transactionID === stmt.transactionID)
      : null;
  },
);
export const selectContentionDetailsForStatement = createSelector(
  selectClusterLocks,
  selectActiveTransactions,
  selectActiveTxnFromStmt,
  getContentionDetailsFromLocksAndTxns,
);
