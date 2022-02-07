// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import moment from "moment";
import { byteArrayToUuid } from "src/sessions";
import { TimestampToMoment } from "src/util";
import { ActiveTransaction } from ".";
import { SessionsResponse, ActiveStatementPhase } from "./types";
import { ActiveStatement, ActiveStatementFilters } from "./types";

export const ACTIVE_STATEMENT_SEARCH_PARAM = "q";

export function filterActiveStatements(
  statements: ActiveStatement[],
  filters: ActiveStatementFilters,
  search?: string,
): ActiveStatement[] {
  let filteredStatements = statements.filter(
    stmt => filters.app === "" || stmt.application === filters.app,
  );

  if (search && search !== "") {
    filteredStatements = filteredStatements.filter(stmt =>
      stmt.query.includes(search),
    );
  }

  return filteredStatements;
}

export function activeStatementsFromSessions(
  sessionsResponse: SessionsResponse,
): ActiveStatement[] {
  const activeQueries: ActiveStatement[] = [];
  if (sessionsResponse.sessions != null) {
    sessionsResponse.sessions.forEach(session => {
      session.active_queries.forEach(query => {
        activeQueries.push({
          executionID: query.id,
          transactionID: byteArrayToUuid(query.txn_id),
          sessionID: byteArrayToUuid(session.id),
          query: query.sql?.length > 0 ? query.sql : query.sql_no_constants,
          status:
            query.phase === ActiveStatementPhase.EXECUTING
              ? "Executing"
              : "Waiting",
          start: TimestampToMoment(query.start),
          elapsedTimeSeconds: moment
            .utc()
            .diff(TimestampToMoment(query.start), "seconds"),
          application: session.application_name,
          user: session.username,
          clientAddress: session.client_address,
        });
      });
    });
  }
  return activeQueries;
}

export function appsFromActiveStatements(
  statements: ActiveStatement[],
): string[] {
  return Array.from(
    statements.reduce(
      (apps, stmt) => apps.add(stmt.application),
      new Set<string>(),
    ),
  );
}

export function appsFromActiveTransactions(
  txns: ActiveTransaction[],
): string[] {
  return Array.from(
    txns.reduce((apps, txn) => apps.add(txn.application), new Set<string>()),
  );
}

export function activeTransactionsFromSessions(
  sessionsResponse: SessionsResponse,
): ActiveTransaction[] {
  const activeTxns: ActiveTransaction[] = [];

  if (sessionsResponse.sessions == null) return activeTxns;

  const activeStmts = activeStatementsFromSessions(sessionsResponse);

  sessionsResponse.sessions.forEach(session => {
    const activeTxn = session.active_txn;
    if (activeTxn == null) return;

    const executionID = byteArrayToUuid(activeTxn.id);
    // Find most recent statement.
    const mostRecentStmt = activeStmts.find(
      stmt => stmt.transactionID === executionID,
    );

    activeTxns.push({
      executionID,
      sessionID: byteArrayToUuid(session.id),
      mostRecentStatement: mostRecentStmt,
      status: "Executing",
      start: TimestampToMoment(activeTxn.start),
      elapsedTimeSeconds: moment
        .utc()
        .diff(TimestampToMoment(activeTxn.start), "seconds"),
      application: session.application_name,
      retries: activeTxn.num_retries,
      statementCount: activeTxn.num_statements_executed,
    });
  });
  return activeTxns;
}
