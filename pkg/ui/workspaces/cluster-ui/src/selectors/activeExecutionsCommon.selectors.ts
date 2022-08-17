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
import { ActiveExecutions, SessionsResponse } from "src/activeExecutions/types";
import { ClusterLocksResponse } from "src/api";
import { executionIdAttr, getMatchParamByName } from "src/util";
import {
  getActiveExecutionsFromSessions,
  getWaitTimeByTxnIDFromLocks,
} from "../activeExecutions/activeStatementUtils";

// The functions in this file are agnostic to the different shape of each
// state in db-console and cluster-ui. This file contains selector functions
// and combiners that can be reused across both packages.

export const selectExecutionID = (
  _state: unknown,
  props: RouteComponentProps,
): string | null => getMatchParamByName(props.match, executionIdAttr);

export const selectActiveExecutionsCombiner = (
  sessions: SessionsResponse | null,
  clusterLocks: ClusterLocksResponse | null,
  lastUpdated: moment.Moment,
): ActiveExecutions => {
  if (!sessions) return { statements: [], transactions: [] };

  const waitTimeByTxnID = getWaitTimeByTxnIDFromLocks(clusterLocks);
  const execs = getActiveExecutionsFromSessions(sessions, lastUpdated);

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
