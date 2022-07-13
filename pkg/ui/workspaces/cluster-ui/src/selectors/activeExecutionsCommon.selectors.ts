// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { RouteComponentProps } from "react-router";
import {
  ActiveTransaction,
  ActiveStatement,
  SessionsResponse,
  ExecutionContentionDetails,
  ActiveExecutions,
} from "src/activeExecutions/types";
import { ClusterLocksResponse } from "src/api";
import { executionIdAttr, getMatchParamByName } from "src/util";
import {
  getActiveExecutionsFromSessions,
  getContendedExecutionsFromLocks,
  getWaitTimeByTxnIDFromLocks,
} from "../activeExecutions/activeStatementUtils";

// The functions in this file are agnostic to the different shape of each
// state in db-console and cluster-ui. This file contains selector functions
// and combiners that can be reused across both packages.

export const selectExecutionID = (
  _state: unknown,
  props: RouteComponentProps,
) => getMatchParamByName(props.match, executionIdAttr);

export const selectActiveExecutionsCombiner = (
  sessions: SessionsResponse,
  clusterLocks: ClusterLocksResponse,
  lastUpdated: moment.Moment,
): ActiveExecutions => {
  if (!sessions) return { statements: [], transactions: [] };

  const waitTimeByTxnID = getWaitTimeByTxnIDFromLocks(clusterLocks);
  const execs = getActiveExecutionsFromSessions(sessions, lastUpdated);

  return {
    statements: execs.statements.map(s => ({
      ...s,
      timeSpentWaiting: waitTimeByTxnID[s.transactionID],
    })),
    transactions: execs.transactions.map(t => ({
      ...t,
      timeSpentWaiting: waitTimeByTxnID[t.transactionID],
    })),
  };
};

export const selectActiveTransactionCombiner = (
  transactions: ActiveTransaction[],
  txnExecutionID: string,
): ActiveTransaction | null => {
  if (!transactions || transactions.length === 0) return null;
  return transactions.find(txn => txn.transactionID === txnExecutionID);
};

export const selectActiveStatementCombiner = (
  statements: ActiveStatement[],
  stmtExecutionID: string,
): ActiveStatement => {
  if (!statements || statements.length === 0) return null;
  return statements.find(stmt => stmt.statementID === stmtExecutionID);
};

export const selectContentionDetailsCombiner = (
  clusterLocks: ClusterLocksResponse | null,
  transactions: ActiveTransaction[],
  currentTransaction: ActiveTransaction | null,
): ExecutionContentionDetails | null => {
  if (!currentTransaction || !clusterLocks?.length) {
    return null;
  }

  const txnID = currentTransaction.transactionID;
  const contention = getContendedExecutionsFromLocks(
    transactions,
    clusterLocks,
    txnID,
  );

  if (!contention) return null;

  let waitInsights = null;
  if (contention.requiredLock != null) {
    waitInsights = {
      waitTime: contention.requiredLock.waiters?.find(
        waiter => waiter.id === txnID,
      )?.waitTime,
      databaseName: contention.requiredLock.databaseName,
      indexName: contention.requiredLock.indexName,
      tableName: contention.requiredLock.tableName,
    };
  }

  return {
    waitInsights,
    waitingExecutions: contention?.waiters,
    blockingExecutions: contention?.blockers,
  };
};
