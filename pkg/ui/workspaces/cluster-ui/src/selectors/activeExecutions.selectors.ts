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
  ActiveTransaction,
  ActiveStatement,
  SessionsResponse,
  ExecutionContentionDetails,
  ActiveExecutions,
} from "src/activeExecutions/types";
import { AppState } from "src/store";
import {
  selectActiveExecutionsCombiner,
  selectExecutionID,
  selectActiveTransactionCombiner,
  selectContentionDetailsCombiner,
  selectActiveStatementCombiner,
} from "./activeExecutionsCommon.selectors";

// This file contains selector functions used across active execution
// pages that are specific to cluster-ui.
// They should NOT be exported with the cluster-ui package.

const selectSessions = (state: AppState) => state.adminUI.sessions?.data;

const selectSessionsLastUpdated = (state: AppState) =>
  state.adminUI.sessions?.lastUpdated;

const selectClusterLocks = (state: AppState) =>
  state.adminUI.clusterLocks?.data;

export const selectActiveExecutions = createSelector(
  selectSessions,
  selectClusterLocks,
  selectSessionsLastUpdated,
  selectActiveExecutionsCombiner,
);

export const selectActiveStatements = createSelector(
  selectActiveExecutions,
  (executions: ActiveExecutions) => executions.statements,
);

export const selectActiveStatement = createSelector(
  selectActiveStatements,
  selectExecutionID,
  selectActiveStatementCombiner,
);

export const selectActiveTransactions = createSelector(
  selectActiveExecutions,
  (executions: ActiveExecutions) => executions.transactions,
);

export const selectActiveTransaction = createSelector(
  selectActiveTransactions,
  selectExecutionID,
  selectActiveTransactionCombiner,
);

export const selectContentionDetailsForTransaction = createSelector(
  selectClusterLocks,
  selectActiveTransactions,
  selectActiveTransaction,
  selectContentionDetailsCombiner,
);

const selectActiveTxnFromStmt = createSelector(
  selectActiveStatement,
  selectActiveTransactions,
  (stmt, transactions) => {
    return transactions.find(txn => txn.transactionID === stmt.transactionID);
  },
);

export const selectContentionDetailsForStatement = createSelector(
  selectClusterLocks,
  selectActiveTransactions,
  selectActiveTxnFromStmt,
  selectContentionDetailsCombiner,
);

export const selectAppName = createSelector(
  (state: AppState) => state.adminUI.sessions,
  response => {
    if (!response.data) return null;
    return response.data.internal_app_name_prefix;
  },
);
