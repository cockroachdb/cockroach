// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  ExecutionStatus,
  ActiveExecutions,
  SessionsResponse,
} from "src/activeExecutions/types";
import { ClusterLocksResponse } from "src/api";

import {
  getActiveExecutionsFromSessions,
  getWaitTimeByTxnIDFromLocks,
} from "../activeExecutions";

// The functions in this file are agnostic to the different shape of each
// state in db-console and cluster-ui. This file contains selector functions
// and combiners that can be reused across both packages.

export const selectActiveExecutionsCombiner = (
  sessions: SessionsResponse | null,
  clusterLocks: ClusterLocksResponse | null,
): ActiveExecutions => {
  if (!sessions) return { statements: [], transactions: [] };

  const waitTimeByTxnID = getWaitTimeByTxnIDFromLocks(clusterLocks);
  const execs = getActiveExecutionsFromSessions(sessions);

  return {
    statements: execs.statements.map(s => ({
      ...s,
      status:
        waitTimeByTxnID[s.transactionID] != null
          ? ExecutionStatus.Waiting
          : s.status,
      timeSpentWaiting: waitTimeByTxnID[s.transactionID],
    })),
    transactions: execs.transactions.map(t => ({
      ...t,
      status:
        waitTimeByTxnID[t.transactionID] != null
          ? ExecutionStatus.Waiting
          : t.status,
      timeSpentWaiting: waitTimeByTxnID[t.transactionID],
    })),
  };
};
