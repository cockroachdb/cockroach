// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
  Idle = "Idle",
}
export type ExecutionType = "statement" | "transaction";

export const ActiveStatementPhase =
  protos.cockroach.server.serverpb.ActiveQuery.Phase;
export const SessionStatusType =
  protos.cockroach.server.serverpb.Session.Status;

export interface ActiveExecution {
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

export type ActiveStatement = ActiveExecution &
  Required<Pick<ActiveExecution, "statementID">> & {
    user: string;
    clientAddress: string;
    isFullScan: boolean;
    planGist?: string;
  };

export type ActiveTransaction = ActiveExecution & {
  statementCount: number;
  retries: number;
  lastAutoRetryReason?: string;
  priority: string;
};

export type ActiveExecutions = {
  statements: ActiveStatement[];
  transactions: ActiveTransaction[];
};

export type ActiveStatementFilters = Omit<
  Filters,
  "database" | "sqlType" | "fullScan" | "distributed" | "regions" | "nodes"
>;

export type ActiveTransactionFilters = ActiveStatementFilters;

export type ContendedExecution = Pick<
  ActiveExecution,
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
