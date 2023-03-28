// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import * as protos from "@cockroachlabs/crdb-protobuf-client";
import { Moment } from "moment-timezone";
import { Filters } from "src/queryFilter";

export type SessionsResponse =
  protos.cockroach.server.serverpb.ListSessionsResponse;
export type ActiveStatementResponse =
  protos.cockroach.server.serverpb.ActiveQuery;
export enum ExecutionStatus {
  Waiting = "Waiting",
  Executing = "Executing",
  Preparing = "Preparing",
}
export type ExecutionType = "statement" | "transaction";

export const ActiveStatementPhase =
  protos.cockroach.server.serverpb.ActiveQuery.Phase;
export const SessionStatusType =
  protos.cockroach.server.serverpb.Session.Status;

export interface RecentExecution {
  statementID?: string; // Empty for transactions not currently executing a statement.
  stmtNoConstants?: string; // Empty for transactions not currently executing a statement.
  transactionID: string;
  sessionID: string;
  status: ExecutionStatus;
  start: Moment;
  elapsedTime: moment.Duration;
  application: string;
  database?: string;
  query?: string; // For transactions, this is the latest query executed.
  timeSpentWaiting?: moment.Duration;
}

export type RecentStatement = RecentExecution &
  Required<Pick<RecentExecution, "statementID">> & {
    user: string;
    clientAddress: string;
    isFullScan: boolean;
    planGist?: string;
  };

export type RecentTransaction = RecentExecution & {
  statementCount: number;
  retries: number;
  lastAutoRetryReason?: string;
  priority: string;
};

export type RecentExecutions = {
  statements: RecentStatement[];
  transactions: RecentTransaction[];
};

export type RecentStatementFilters = Omit<
  Filters,
  "database" | "sqlType" | "fullScan" | "distributed" | "regions" | "nodes"
>;

export type RecentTransactionFilters = RecentStatementFilters;

export type ContendedExecution = Pick<
  RecentExecution,
  "status" | "start" | "query"
> & {
  transactionExecutionID: string;
  statementExecutionID: string;
  contentionTime: moment.Duration;
};

export type ExecutionContentionDetails = {
  // Info on the lock the transaction is waiting for.
  waitInsights?: {
    databaseName?: string;
    schemaName?: string;
    tableName?: string;
    indexName?: string;
    waitTime: moment.Duration;
  };
  // Txns waiting for a lock held by this txn.
  waitingExecutions: ContendedExecution[];
  // Txns holding a lock required by this txn.
  blockingExecutions: ContendedExecution[];
};
