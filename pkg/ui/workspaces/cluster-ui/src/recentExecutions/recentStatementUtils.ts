// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import moment from "moment-timezone";
import { byteArrayToUuid } from "src/sessions";
import { TimestampToMoment, unset } from "src/util";
import { RecentTransaction } from ".";
import {
  SessionsResponse,
  ActiveStatementPhase,
  ExecutionStatus,
  RecentTransactionFilters,
  SessionStatusType,
  ContendedExecution,
  RecentExecutions,
  ExecutionContentionDetails,
  RecentExecution,
} from "./types";
import { RecentStatement, RecentStatementFilters } from "./types";
import { ClusterLocksResponse, ClusterLockState } from "src/api";
import { DurationToMomentDuration } from "src/util/convert";

export const RECENT_STATEMENT_SEARCH_PARAM = "q";
export const INTERNAL_APP_NAME_PREFIX = "$ internal";

export function filterRecentStatements(
  statements: RecentStatement[],
  filters: RecentStatementFilters,
  internalAppNamePrefix: string,
  search?: string,
): RecentStatement[] {
  if (statements == null) return [];

  let filteredStatements = statements;

  const isInternal = (statement: RecentStatement) =>
    statement.application.startsWith(internalAppNamePrefix);
  if (filters.app) {
    filteredStatements = filteredStatements.filter(
      (statement: RecentStatement) => {
        const apps = filters.app.toString().split(",");
        let showInternal = false;
        if (apps.includes(internalAppNamePrefix)) {
          showInternal = true;
        }
        if (apps.includes(unset)) {
          apps.push("");
        }
        return (
          (showInternal && isInternal(statement)) ||
          apps.includes(statement.application)
        );
      },
    );
  } else {
    filteredStatements = filteredStatements.filter(
      statement => !isInternal(statement),
    );
  }

  if (filters.executionStatus) {
    filteredStatements = filteredStatements.filter(
      (statement: RecentStatement) => {
        const executionStatuses = filters.executionStatus.toString().split(",");

        return executionStatuses.includes(statement.status);
      },
    );
  }

  if (search) {
    const searchCaseInsensitive = search.toLowerCase();
    filteredStatements = filteredStatements.filter(stmt =>
      stmt.query.toLowerCase().includes(searchCaseInsensitive),
    );
  }

  return filteredStatements;
}

/**
 * getRecentExecutionsFromSessions returns recent statements and
 * transactions from the array of sessions provided.
 * @param sessionsResponse sessions array from which to extract data
 * @returns
 */
export function getRecentExecutionsFromSessions(
  sessionsResponse: SessionsResponse,
): RecentExecutions {
  if (sessionsResponse.sessions == null)
    return { statements: [], transactions: [] };

  const statements: RecentStatement[] = [];
  const transactions: RecentTransaction[] = [];

  sessionsResponse.sessions
    .filter(
      session =>
        session.status !== SessionStatusType.CLOSED &&
        (session.active_txn || session.active_queries?.length !== 0),
    )
    .forEach(session => {
      const sessionID = byteArrayToUuid(session.id);

      let activeStmt: RecentStatement = null;
      if (session.active_queries.length) {
        // There will only ever be one query in this array.
        const query = session.active_queries[0];
        const queryTxnID = byteArrayToUuid(query.txn_id);
        activeStmt = {
          statementID: query.id,
          stmtNoConstants: query.sql_no_constants,
          transactionID: queryTxnID,
          sessionID,
          // VIEWACTIVITYREDACTED users will not have access to the full SQL query.
          query: query.sql?.length > 0 ? query.sql : query.sql_no_constants,
          status:
            query.phase === ActiveStatementPhase.EXECUTING
              ? ExecutionStatus.Executing
              : ExecutionStatus.Preparing,
          start: TimestampToMoment(query.start),
          elapsedTime: DurationToMomentDuration(query.elapsed_time),
          application: session.application_name,
          database: query.database,
          user: session.username,
          clientAddress: session.client_address,
          isFullScan: query.is_full_scan || false, // Or here is for conversion in case the field is null.
          planGist: query.plan_gist,
        };

        statements.push(activeStmt);
      }

      const activeTxn = session.active_txn;
      if (!activeTxn) return;

      transactions.push({
        transactionID: byteArrayToUuid(activeTxn.id),
        sessionID,
        query:
          activeStmt?.query ??
          (activeTxn.num_statements_executed
            ? session.last_active_query
            : null),
        statementID: activeStmt?.statementID,
        status: "Executing" as ExecutionStatus,
        start: TimestampToMoment(activeTxn.start),
        elapsedTime: DurationToMomentDuration(activeTxn.elapsed_time),
        application: session.application_name,
        retries: activeTxn.num_auto_retries,
        statementCount: activeTxn.num_statements_executed,
        lastAutoRetryReason: activeTxn.last_auto_retry_reason,
        priority: activeTxn.priority,
      });
    });

  return {
    transactions,
    statements,
  };
}

export function getAppsFromRecentExecutions(
  executions: RecentExecution[] | null,
  internalAppNamePrefix: string,
): string[] {
  if (executions == null) return [];

  const uniqueAppNames = new Set(
    executions.map(e => {
      if (e.application.startsWith(internalAppNamePrefix)) {
        return internalAppNamePrefix;
      }
      return e.application ? e.application : unset;
    }),
  );

  return Array.from(uniqueAppNames).sort();
}

export function filterRecentTransactions(
  txns: RecentTransaction[] | null,
  filters: RecentTransactionFilters,
  internalAppNamePrefix: string,
  search?: string,
): RecentTransaction[] {
  if (txns == null) return [];

  let filteredTxns = txns;

  const isInternal = (txn: RecentTransaction) =>
    txn.application.startsWith(internalAppNamePrefix);
  if (filters.app) {
    filteredTxns = filteredTxns.filter((txn: RecentTransaction) => {
      const apps = filters.app.toString().split(",");
      let showInternal = false;
      if (apps.includes(internalAppNamePrefix)) {
        showInternal = true;
      }
      if (apps.includes(unset)) {
        apps.push("");
      }

      return (
        (showInternal && isInternal(txn)) || apps.includes(txn.application)
      );
    });
  } else {
    filteredTxns = filteredTxns.filter(txn => !isInternal(txn));
  }

  if (filters.executionStatus) {
    filteredTxns = filteredTxns.filter((txn: RecentTransaction) => {
      const executionStatuses = filters.executionStatus.toString().split(",");

      return executionStatuses.includes(txn.status);
    });
  }

  if (search) {
    const searchCaseInsensitive = search.toLowerCase();
    filteredTxns = filteredTxns.filter(txn =>
      txn.query?.toLowerCase().includes(searchCaseInsensitive),
    );
  }

  return filteredTxns;
}

export function getContendedExecutionsForTxn(
  transactions: RecentTransaction[],
  locks: ClusterLockState[],
  txnExecID: string, // Txn we are returning contention info for.
): {
  waiters: ContendedExecution[];
  blockers: ContendedExecution[];
  requiredLock: ClusterLockState;
} | null {
  if (
    !transactions ||
    transactions.length === 0 ||
    !locks ||
    locks.length === 0
  ) {
    return null;
  }

  const txnByID: Record<string, RecentTransaction> = {};
  transactions.forEach(txn => (txnByID[txn.transactionID] = txn));

  const waiters: ContendedExecution[] = [];
  const blockers: ContendedExecution[] = [];
  let requiredLock: ClusterLockState | null = null;

  locks.forEach(lock => {
    if (lock.lockHolderTxnID === txnExecID) {
      lock.waiters
        .filter(waiter => txnByID[waiter.id])
        .forEach(waiter => {
          const activeTxn = txnByID[waiter.id];
          waiters.push({
            ...activeTxn,
            statementExecutionID: activeTxn.statementID,
            transactionExecutionID: activeTxn.transactionID,
            contentionTime: waiter.waitTime,
          });
        });
      return;
    }

    const isWaiter = lock.waiters.map(waiter => waiter.id).includes(txnExecID);
    if (!isWaiter) return;

    // We just need to set this for information on the db, index, schema etc.
    requiredLock = lock;

    const lockHolder = txnByID[lock.lockHolderTxnID];

    if (!lockHolder) return;

    blockers.push({
      ...lockHolder,
      statementExecutionID: lockHolder.statementID,
      transactionExecutionID: lockHolder.transactionID,
      contentionTime: lock.holdTime,
    });
  });

  // For VIEWACTIVITYREDACTED, we won't have info on relating txns
  // for a txn waiting for a lock. We can still include information
  // on the blocked schema, index, and db.
  if (waiters.length === 0 && blockers.length === 0 && !requiredLock) {
    return null;
  }

  return {
    waiters,
    blockers,
    requiredLock,
  };
}

export function getWaitTimeByTxnIDFromLocks(
  locks: ClusterLockState[],
): Record<string, moment.Duration> {
  if (!locks || locks.length === 0) return {};

  const waitTimeRecord: Record<string, moment.Duration> = {};

  locks.forEach(lock =>
    lock.waiters?.forEach(waiter => {
      if (!waiter.waitTime) return;

      if (!waitTimeRecord[waiter.id]) {
        waitTimeRecord[waiter.id] = moment.duration(0);
      }

      waitTimeRecord[waiter.id].add(waiter.waitTime);
    }),
  );

  return waitTimeRecord;
}

export const getRecentTransaction = (
  transactions: RecentTransaction[],
  txnExecutionID: string,
): RecentTransaction | null => {
  if (!transactions || transactions.length === 0) return null;
  return transactions.find(txn => txn.transactionID === txnExecutionID);
};

export const getRecentStatement = (
  statements: RecentStatement[],
  stmtExecutionID: string,
): RecentStatement => {
  if (!statements || statements.length === 0) return null;
  return statements.find(stmt => stmt.statementID === stmtExecutionID);
};

export const getContentionDetailsFromLocksAndTxns = (
  clusterLocks: ClusterLocksResponse | null,
  transactions: RecentTransaction[],
  currentTransaction: RecentTransaction | null,
): ExecutionContentionDetails | null => {
  if (!currentTransaction || !clusterLocks?.length) {
    return null;
  }

  const txnID = currentTransaction.transactionID;
  const contention = getContendedExecutionsForTxn(
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
      schemaName: contention.requiredLock.schemaName,
    };
  }

  return {
    waitInsights,
    waitingExecutions: contention?.waiters,
    blockingExecutions: contention?.blockers,
  };
};
