// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";

import { ClusterLocksResponse, ClusterLockState } from "src/api";
import { byteArrayToUuid } from "src/sessions";
import { TimestampToMoment, unset } from "src/util";
import { DurationToMomentDuration } from "src/util/convert";

import {
  SessionsResponse,
  ActiveStatementPhase,
  ExecutionStatus,
  ActiveTransactionFilters,
  SessionStatusType,
  ContendedExecution,
  ActiveExecutions,
  ExecutionContentionDetails,
  ActiveExecution,
  ActiveStatement,
  ActiveStatementFilters,
} from "./types";

import { ActiveTransaction } from ".";

export const ACTIVE_STATEMENT_SEARCH_PARAM = "q";
export const INTERNAL_APP_NAME_PREFIX = "$ internal";

export function filterActiveStatements(
  statements: ActiveStatement[],
  filters: ActiveStatementFilters,
  internalAppNamePrefix: string,
  search?: string,
): ActiveStatement[] {
  if (statements == null) return [];

  let filteredStatements = statements;

  const isInternal = (statement: ActiveStatement) =>
    statement.application.startsWith(internalAppNamePrefix);
  if (filters.app) {
    filteredStatements = filteredStatements.filter(
      (statement: ActiveStatement) => {
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
      (statement: ActiveStatement) => {
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
 * getActiveExecutionsFromSessions returns Active statements and
 * transactions from the array of sessions provided.
 * @param sessionsResponse sessions array from which to extract data
 * @returns
 */
export function getActiveExecutionsFromSessions(
  sessionsResponse: SessionsResponse,
): ActiveExecutions {
  if (sessionsResponse.sessions == null)
    return { statements: [], transactions: [] };

  const statements: ActiveStatement[] = [];
  const transactions: ActiveTransaction[] = [];

  sessionsResponse.sessions
    .filter(
      session =>
        session.status !== SessionStatusType.CLOSED &&
        (session.active_txn || session.active_queries?.length !== 0),
    )
    .forEach(session => {
      const sessionID = byteArrayToUuid(session.id);

      let activeStmt: ActiveStatement = null;
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
        status:
          activeStmt != null ? ExecutionStatus.Executing : ExecutionStatus.Idle,
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

export function getAppsFromActiveExecutions(
  executions: ActiveExecution[] | null,
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

export function filterActiveTransactions(
  txns: ActiveTransaction[] | null,
  filters: ActiveTransactionFilters,
  internalAppNamePrefix: string,
  search?: string,
): ActiveTransaction[] {
  if (txns == null) return [];

  let filteredTxns = txns;

  const isInternal = (txn: ActiveTransaction) =>
    txn.application.startsWith(internalAppNamePrefix);
  if (filters.app) {
    filteredTxns = filteredTxns.filter((txn: ActiveTransaction) => {
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
    filteredTxns = filteredTxns.filter((txn: ActiveTransaction) => {
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
  transactions: ActiveTransaction[],
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

  const txnByID: Record<string, ActiveTransaction> = {};
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

export const getActiveTransaction = (
  transactions: ActiveTransaction[],
  txnExecutionID: string,
): ActiveTransaction | null => {
  if (!transactions || transactions.length === 0) return null;
  return transactions.find(txn => txn.transactionID === txnExecutionID);
};

export const getActiveStatement = (
  statements: ActiveStatement[],
  stmtExecutionID: string,
): ActiveStatement => {
  if (!statements || statements.length === 0) return null;
  return statements.find(stmt => stmt.statementID === stmtExecutionID);
};

export const getContentionDetailsFromLocksAndTxns = (
  clusterLocks: ClusterLocksResponse | null,
  transactions: ActiveTransaction[],
  currentTransaction: ActiveTransaction | null,
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
