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
  SessionsResponse,
  TransactionContentionDetails,
} from "src/activeExecutions/types";
import { ClusterLocksResponse } from "src/api";
import { executionIdAttr, getMatchParamByName } from "src/util";
import {
  getActiveTransactionsFromSessions,
  getContendedTransactionsFromLocks,
} from "../activeExecutions/activeStatementUtils";

export const selectActiveTransactionsCombiner = (
  sessions: SessionsResponse,
  lastUpdated: moment.Moment,
): ActiveTransaction[] => {
  if (!sessions) return [];

  return getActiveTransactionsFromSessions(sessions, lastUpdated);
};

export const selectActiveTransactionCombiner = (
  transactions: ActiveTransaction[],
  props: RouteComponentProps,
): ActiveTransaction | null => {
  if (!transactions || transactions.length === 0) return null;
  const id = getMatchParamByName(props.match, executionIdAttr);
  return transactions.find(txn => txn.executionID === id);
};

export const selectContentionDetailsCombiner = (
  clusterLocks: ClusterLocksResponse | null,
  transactions: ActiveTransaction[],
  currentTransaction: ActiveTransaction | null,
): TransactionContentionDetails | null => {
  if (!currentTransaction || !clusterLocks?.length) {
    return null;
  }

  const txnID = currentTransaction.executionID;
  const contention = getContendedTransactionsFromLocks(
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
    waitingTxns: contention?.waiters,
    blockingTxns: contention?.blockers,
  };
};
