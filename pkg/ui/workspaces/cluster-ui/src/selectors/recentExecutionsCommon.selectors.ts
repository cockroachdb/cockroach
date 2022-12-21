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
  RecentStatement,
  SessionsResponse,
} from "src/recentExecutions/types";
import { ClusterLocksResponse } from "src/api";
import {
  getRecentExecutionsFromSessions,
  getWaitTimeByTxnIDFromLocks,
} from "../recentExecutions";

// The functions in this file are agnostic to the different shape of each
// state in db-console and cluster-ui. This file contains selector functions
// and combiners that can be reused across both packages.

export const selectActiveExecutionsCombiner = (
  sessions: SessionsResponse | null,
  clusterLocks: ClusterLocksResponse | null,
): RecentExecutions => {
  if (!sessions) return { statements: [], transactions: [] };

  const waitTimeByTxnID = getWaitTimeByTxnIDFromLocks(clusterLocks);
  const execs = getRecentExecutionsFromSessions(sessions);

  return {
    statements: execs.statements.map(s => ({
      ...s,
      status: waitTimeByTxnID[s.transactionID] != null ? "Waiting" : s.status,
      timeSpentWaiting: waitTimeByTxnID[s.transactionID],
    })),
    transactions: execs.transactions.map(t => ({
      ...t,
      status: waitTimeByTxnID[t.transactionID] != null ? "Waiting" : t.status,
      timeSpentWaiting: waitTimeByTxnID[t.transactionID],
    })),
  };
};

export const selectHistoricalExecutionsCombiner = (
  recentStatements: RecentStatement[] | null,
  clusterLocks: ClusterLocksResponse | null,
): RecentExecutions => {
  if (!recentStatements) return { statements: [], transactions: [] };

  const waitTimeByTxnID = getWaitTimeByTxnIDFromLocks(clusterLocks);

  return {
    statements: recentStatements.map(s => ({
      ...s,
      status: waitTimeByTxnID[s.transactionID] != null ? "Waiting" : s.status,
      timeSpentWaiting: waitTimeByTxnID[s.transactionID],
    })),
    // TODO(amy): implement recently completed transactions
    transactions: [],
  };
};

export const selectRecentExecutionsCombiner = (
  recentlyCompletedExecutions: RecentExecutions | null,
  activeExecutions: RecentExecutions | null,
): RecentExecutions => {
  if (!recentlyCompletedExecutions && !activeExecutions)
    return { statements: [], transactions: [] };

  return {
    // Create a new set to remove duplicates
    statements: [
      ...new Set(
        (activeExecutions.statements || []).concat(
          recentlyCompletedExecutions.statements || [],
        ),
      ),
    ],
    transactions: activeExecutions.transactions || [],
  };
};
