// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import {
  RecentExecutions,
  selectRecentExecutionsCombiner,
  getRecentStatement,
  getRecentTransaction,
  getContentionDetailsFromLocksAndTxns,
  selectExecutionID,
  ExecutionStatus,
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

export const selectRecentExecutions = createSelector(
  selectSessions,
  selectClusterLocks,
  selectRecentExecutionsCombiner,
);

export const selectRecentStatements = createSelector(
  selectRecentExecutions,
  (executions: RecentExecutions) => executions.statements,
);

export const selectExecutionStatus = () => {
  const execStatuses: string[] = [];
  for (const execStatus in ExecutionStatus) {
    execStatuses.push(execStatus);
  }
  return execStatuses;
};

export const selectRecentStatement = createSelector(
  selectRecentStatements,
  selectExecutionID,
  getRecentStatement,
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

export const selectRecentTransactions = createSelector(
  selectRecentExecutions,
  (executions: RecentExecutions) => executions.transactions,
);

export const selectRecentTransaction = createSelector(
  selectRecentTransactions,
  selectExecutionID,
  getRecentTransaction,
);

export const selectContentionDetailsForTransaction = createSelector(
  selectClusterLocks,
  selectRecentTransactions,
  selectRecentTransaction,
  getContentionDetailsFromLocksAndTxns,
);

const selectRecentTxnFromStmt = createSelector(
  selectRecentStatement,
  selectRecentTransactions,
  (stmt, transactions) => {
    return stmt
      ? transactions.find(txn => txn.transactionID === stmt.transactionID)
      : null;
  },
);
export const selectContentionDetailsForStatement = createSelector(
  selectClusterLocks,
  selectRecentTransactions,
  selectRecentTxnFromStmt,
  getContentionDetailsFromLocksAndTxns,
);
