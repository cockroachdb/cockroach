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
  executeInternalSql, LARGE_RESULT_SIZE,
  SqlExecutionRequest,
  sqlResultsAreEmpty,
} from "./sqlApi";
import moment from "moment";
import { ExecutionStatus, RecentStatement } from "../recentExecutions";

export type RecentStatementsColumns = {
  stmt_id: string,
  stmt_no_constants: string,
  txn_id: string,
  session_id: string,
  query: string,
  status: string,
  start_time: string,
  elapsed_time: string,
  app_name: string,
  database_name: string,
  user_name: string,
  client_address: string,
  is_full_scan: string,
  plan_gist: string
};

const recentStatementsQuery = (id: string): string =>
  `SELECT
    stmt_id,
    stmt_no_constants,
    txn_id,
    session_id,
    query,
    status,
    start_time,
    elapsed_time,
    app_name,
    database_name,
    user_name,
    client_address,
    is_full_scan,
    plan_gist
  FROM crdb_internal.node_recent_statements
  WHERE stmt_id > ${id}
  ORDER BY stmt_id ASC
  LIMIT 100`

export type RecentStatementsRequestKey = { id: string };

export const recentStatementsRequest = (id: string): SqlExecutionRequest => {
  return {
  statements: [
    {
      sql: recentStatementsQuery(id),
    },
  ],
  execute: true,
  max_result_size: LARGE_RESULT_SIZE,
}
};

function stringToExecutionStatus(status: string): ExecutionStatus {
  switch (status) {
    case "PREPARING":
      return "Preparing"
    case "EXECUTING":
      return "Executing"
    case "CANCELED":
      return "Canceled"
    case "TIMED_OUT":
      return "Timed Out"
    case "COMPLETED":
      return "Completed"
    case "FAILED":
      return "Failed"
  }
}


export async function getRecentStatements(key: RecentStatementsRequestKey): Promise<RecentStatement[]> {
  return executeInternalSql<RecentStatementsColumns>(recentStatementsRequest(key.id)).then(
    result => {
      if (sqlResultsAreEmpty(result)) {
        if (result.error) {
          throw result.error;
        }
        return [];
      }
      const recentStatements: RecentStatement[] = result.execution.txn_results[0].rows.map(
        row => ({
          statementID: row.stmt_id,
          stmtNoConstants: row.stmt_no_constants,
          transactionID: row.txn_id,
          sessionID: row.session_id,
          query: row.query,
          status: stringToExecutionStatus(row.status),
          start: moment.utc(row.start_time),
          elapsedTime: moment.duration(row.elapsed_time),
          application: row.app_name,
          database: row.database_name,
          user: row.user_name,
          clientAddress: row.client_address,
          isFullScan: row.is_full_scan === 'true',
          planGist: row.plan_gist
        })
      );
      return recentStatements
    }
  )
}
