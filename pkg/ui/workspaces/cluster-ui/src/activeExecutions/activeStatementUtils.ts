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
import { TimestampToMoment } from "src/util";
import { ActiveTransaction } from ".";
import {
  SessionsResponse,
  ActiveStatementPhase,
  ExecutionStatus,
  ActiveTransactionFilters,
} from "./types";
import { ActiveStatement, ActiveStatementFilters } from "./types";

export const ACTIVE_STATEMENT_SEARCH_PARAM = "q";

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
        if (apps.includes("(unset)")) {
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
    session.active_queries.forEach(query => {
      activeQueries.push({
        executionID: query.id,
        transactionID: byteArrayToUuid(query.txn_id),
        sessionID: byteArrayToUuid(session.id),
        // VIEWACTIVITYREDACTED users will not have access to the full SQL query.
        query: query.sql?.length > 0 ? query.sql : query.sql_no_constants,
        status:
          query.phase === ActiveStatementPhase.EXECUTING
            ? "Executing"
            : "Preparing",
        start: TimestampToMoment(query.start),
        elapsedTimeSeconds: time.diff(
          TimestampToMoment(query.start),
          "seconds",
        ),
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
      return s.application ? s.application : "(unset)";
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
      if (apps.includes("(unset)")) {
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
      txn => !search || txn.mostRecentStatement?.query.includes(search),
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
      return t.application ? t.application : "(unset)";
    }),
  );

  return Array.from(uniqueAppNames).sort();
}

export function getActiveTransactionsFromSessions(
  sessionsResponse: SessionsResponse,
  lastUpdated: Moment,
): ActiveTransaction[] {
  if (sessionsResponse.sessions == null) return [];

  const time = lastUpdated || moment.utc();

  const activeStmts = getActiveStatementsFromSessions(sessionsResponse, time);
  const activeStmtByTxnID: Record<string, ActiveStatement> = {};

  activeStmts.forEach(stmt => {
    activeStmtByTxnID[stmt.transactionID] = stmt;
  });

  return sessionsResponse.sessions
    .filter(session => session.active_txn)
    .map(session => {
      const activeTxn = session.active_txn;

      const executionID = byteArrayToUuid(activeTxn.id);
      // Find most recent statement.
      const mostRecentStmt = activeStmtByTxnID[executionID];

      return {
        executionID,
        sessionID: byteArrayToUuid(session.id),
        mostRecentStatement: mostRecentStmt,
        status: "Executing" as ExecutionStatus,
        start: TimestampToMoment(activeTxn.start),
        elapsedTimeSeconds: time.diff(
          TimestampToMoment(activeTxn.start),
          "seconds",
        ),
        application: session.application_name,
        retries: activeTxn.num_auto_retries,
        statementCount: activeTxn.num_statements_executed,
      };
    });
}
