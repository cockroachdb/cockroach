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
  RecentExecutions,
  RecentTransaction,
  ExecutionStatus,
} from "src/recentExecutions/types";
import { AppState } from "src/store";
import { selectRecentExecutionsCombiner } from "src/selectors/recentExecutionsCommon.selectors";
import { selectExecutionID } from "src/selectors/common";
import {
  getRecentTransaction,
  getContentionDetailsFromLocksAndTxns,
  getRecentStatement,
} from "src/recentExecutions/recentStatementUtils";

// This file contains selector functions used across recent execution
// pages that are specific to cluster-ui.
// They should NOT be exported with the cluster-ui package.

const selectSessions = (state: AppState) => state.adminUI.sessions?.data;

const selectClusterLocks = (state: AppState) =>
  state.adminUI.clusterLocks?.data;

export const selectRecentExecutions = createSelector(
  selectSessions,
  selectClusterLocks,
  selectRecentExecutionsCombiner,
);

export const selectRecentStatements = createSelector(
  selectRecentExecutions,
  (executions: RecentExecutions) => executions.statements,
);

// Recent Statement and Transaction executions share the same set of executions
// statuses so we're able to select them from a single function.
export const selectExecutionStatus = () => {
  const execTypes: string[] = [];
  for (const execType in ExecutionStatus) {
    execTypes.push(execType);
  }
  return execTypes;
};

export const selecteRecentStatement = createSelector(
  selectRecentStatements,
  selectExecutionID,
  getRecentStatement,
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
  selecteRecentStatement,
  selectRecentTransactions,
  (stmt, transactions) => {
    return transactions.find(txn => txn.transactionID === stmt.transactionID);
  },
);

export const selectContentionDetailsForStatement = createSelector(
  selectClusterLocks,
  selectRecentTransactions,
  selectRecentTxnFromStmt,
  getContentionDetailsFromLocksAndTxns,
);

export const selectAppName = createSelector(
  (state: AppState) => state.adminUI.sessions,
  response => {
    if (!response.data) return null;
    return response.data.internal_app_name_prefix;
  },
);
