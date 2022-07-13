// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import moment, { Moment } from "moment";
import { byteArrayToUuid } from "src/sessions";
import { TimestampToMoment, unset } from "src/util";
import { ActiveTransaction } from ".";
import {
  SessionsResponse,
  ActiveStatementPhase,
  ExecutionStatus,
  ActiveTransactionFilters,
  SessionStatusType,
  ContendedExecution,
  ActiveExecutions,
} from "./types";
import { ActiveStatement, ActiveStatementFilters } from "./types";
import { ClusterLockState } from "src/api";

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

  if (search) {
    filteredStatements = filteredStatements.filter(stmt =>
      stmt.query.includes(search),
    );
  }

  return filteredStatements;
}

export function getActiveStatementsFromSessions(
  sessionsResponse: SessionsResponse,
  lastUpdated: Moment | null,
): ActiveStatement[] {
  const activeQueries: ActiveStatement[] = [];
  if (sessionsResponse.sessions == null) {
    return activeQueries;
  }

  const time = lastUpdated || moment.utc();

  sessionsResponse.sessions.forEach(session => {
    if (session.status === SessionStatusType.CLOSED) return;
    session.active_queries.forEach(query => {
      activeQueries.push({
        statementID: query.id,
        transactionID: byteArrayToUuid(query.txn_id),
        sessionID: byteArrayToUuid(session.id),
        // VIEWACTIVITYREDACTED users will not have access to the full SQL query.
        query: query.sql?.length > 0 ? query.sql : query.sql_no_constants,
        status:
          query.phase === ActiveStatementPhase.EXECUTING
            ? "Executing"
            : "Preparing",
        start: TimestampToMoment(query.start),
        elapsedTimeMillis: time.diff(TimestampToMoment(query.start), "ms"),
        application: session.application_name,
        user: session.username,
        clientAddress: session.client_address,
      });
    });
  });

  return activeQueries;
}

export function getAppsFromActiveStatements(
  statements: ActiveStatement[] | null,
  internalAppNamePrefix: string,
): string[] {
  if (statements == null) return [];

  const uniqueAppNames = new Set(
    statements.map(s => {
      if (s.application.startsWith(internalAppNamePrefix)) {
        return internalAppNamePrefix;
      }
      return s.application ? s.application : unset;
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

  if (search) {
    filteredTxns = filteredTxns.filter(
      txn => !search || txn.query?.includes(search),
    );
  }

  return filteredTxns;
}

export function getAppsFromActiveTransactions(
  txns: ActiveTransaction[],
  internalAppNamePrefix: string,
): string[] {
  if (txns == null) return [];

  const uniqueAppNames = new Set(
    txns.map(t => {
      if (t.application.startsWith(internalAppNamePrefix)) {
        return internalAppNamePrefix;
      }
      return t.application ? t.application : unset;
    }),
  );

  return Array.from(uniqueAppNames).sort();
}

/**
 * getActiveExecutionsFromSessions returns active statements and
 * transactions from the array of sessions provided.
 * @param sessionsResponse sessions array from which to extract data
 * @param lastUpdated the time the sessions data was last updated
 * @returns
 */
export function getActiveExecutionsFromSessions(
  sessionsResponse: SessionsResponse,
  lastUpdated: Moment,
): ActiveExecutions {
  if (sessionsResponse.sessions == null)
    return { statements: [], transactions: [] };

  const time = lastUpdated || moment.utc();

  const statements = getActiveStatementsFromSessions(sessionsResponse, time);
  const activeStmtByTxnID: Record<string, ActiveStatement> = {};

  statements.forEach(stmt => {
    activeStmtByTxnID[stmt.transactionID] = stmt;
  });

  const transactions = sessionsResponse.sessions
    .filter(
      session =>
        session.status !== SessionStatusType.CLOSED && session.active_txn,
    )
    .map(session => {
      const activeTxn = session.active_txn;

      const executionID = byteArrayToUuid(activeTxn.id);
      // Find most recent statement.
      const mostRecentStmt = activeStmtByTxnID[executionID];

      return {
        transactionID: executionID,
        sessionID: byteArrayToUuid(session.id),
        query: mostRecentStmt?.query,
        statementID: mostRecentStmt?.statementID,
        status: "Executing" as ExecutionStatus,
        start: TimestampToMoment(activeTxn.start),
        elapsedTimeMillis: time.diff(TimestampToMoment(activeTxn.start), "ms"),
        application: session.application_name,
        retries: activeTxn.num_auto_retries,
        statementCount: activeTxn.num_statements_executed,
      };
    });
  return {
    transactions,
    statements,
  };
}

export function getContendedExecutionsFromLocks(
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
    }

    if (lock.waiters.map(waiter => waiter.id).includes(txnExecID)) {
      const activeTxn = txnByID[lock.lockHolderTxnID];
      if (!activeTxn) return;

      // We just need to set this for information on the db, index, schema etc.
      requiredLock = lock;

      blockers.push({
        ...activeTxn,
        statementExecutionID: activeTxn.statementID,
        transactionExecutionID: activeTxn.transactionID,
        contentionTime: lock.holdTime,
      });
    }
  });

  if (waiters.length === 0 && blockers.length === 0) {
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
