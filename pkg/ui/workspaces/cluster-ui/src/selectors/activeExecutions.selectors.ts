// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createSelector } from "reselect";

import {
  getActiveTransaction,
  getContentionDetailsFromLocksAndTxns,
  getActiveStatement,
} from "src/activeExecutions/activeStatementUtils";
import { ActiveExecutions } from "src/activeExecutions/types";
import { selectActiveExecutionsCombiner } from "src/selectors/activeExecutionsCommon.selectors";
import { selectExecutionID } from "src/selectors/common";
import { AppState } from "src/store";

// This file contains selector functions used across active execution
// pages that are specific to cluster-ui.
// They should NOT be exported with the cluster-ui package.

const selectSessions = (state: AppState) => state.adminUI?.sessions?.data;

const selectClusterLocks = (state: AppState) =>
  state.adminUI?.clusterLocks?.data?.results;

export const selectClusterLocksMaxApiSizeReached = (state: AppState) =>
  !!state.adminUI?.clusterLocks?.data?.maxSizeReached;

export const selectActiveExecutions = createSelector(
  selectSessions,
  selectClusterLocks,
  selectActiveExecutionsCombiner,
);

export const selectActiveStatements = createSelector(
  selectActiveExecutions,
  (executions: ActiveExecutions) => executions.statements,
);

export const selecteActiveStatement = createSelector(
  selectActiveStatements,
  selectExecutionID,
  getActiveStatement,
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
  selecteActiveStatement,
  selectActiveTransactions,
  (stmt, transactions) => {
    return transactions.find(txn => txn.transactionID === stmt.transactionID);
  },
);

export const selectContentionDetailsForStatement = createSelector(
  selectClusterLocks,
  selectActiveTransactions,
  selectActiveTxnFromStmt,
  getContentionDetailsFromLocksAndTxns,
);

export const selectAppName = createSelector(
  (state: AppState) => state.adminUI?.sessions,
  response => {
    if (!response?.data) return null;
    return response.data.internal_app_name_prefix;
  },
);
