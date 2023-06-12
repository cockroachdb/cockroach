// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import moment from "moment-timezone";
import { Duration } from "src/util/format";
import {
  executeInternalSql,
  LARGE_RESULT_SIZE,
  SqlExecutionRequest,
  sqlResultsAreEmpty,
} from "src/api";

export type StatementDiagnosticsReport = {
  id: string;
  statement_fingerprint: string;
  completed: boolean;
  statement_diagnostics_id?: string;
  requested_at: moment.Moment;
  min_execution_latency?: moment.Duration;
  expires_at?: moment.Moment;
};

export type StatementDiagnosticsResponse = StatementDiagnosticsReport[];

export function getStatementDiagnosticsReports(): Promise<StatementDiagnosticsResponse> {
  const req: SqlExecutionRequest = {
    statements: [
      {
        sql: `SELECT
      id::STRING,
      statement_fingerprint,
      completed,
      statement_diagnostics_id::STRING,
      requested_at,
      min_execution_latency,
      expires_at
    FROM
      system.statement_diagnostics_requests
    WHERE
      expires_at > now() OR expires_at IS NULL OR completed = true`,
      },
    ],
    execute: true,
    max_result_size: LARGE_RESULT_SIZE,
  };

  return executeInternalSql<StatementDiagnosticsReport>(req).then(res => {
    // If request succeeded but query failed, throw error (caught by saga/cacheDataReducer).
    if (res.error) {
      throw res.error;
    }

    if (sqlResultsAreEmpty(res)) {
      return [];
    }

    return res.execution.txn_results[0].rows;
  });
}

type CheckPendingStmtDiagnosticRow = {
  count: number;
};

export type InsertStmtDiagnosticRequest = {
  stmtFingerprint: string;
  samplingProbability?: number;
  minExecutionLatencySeconds?: number;
  expiresAfterSeconds?: number;
};

export type InsertStmtDiagnosticResponse = {
  req_resp: boolean;
};

export function createStatementDiagnosticsReport({
  stmtFingerprint,
  samplingProbability,
  minExecutionLatencySeconds,
  expiresAfterSeconds,
}: InsertStmtDiagnosticRequest): Promise<InsertStmtDiagnosticResponse> {
  const args: any = [stmtFingerprint];

  if (samplingProbability) {
    args.push(samplingProbability);
  } else {
    args.push(0);
  }
  if (minExecutionLatencySeconds) {
    args.push(Duration(minExecutionLatencySeconds * 1e9));
  } else {
    args.push("0");
  }
  if (expiresAfterSeconds && expiresAfterSeconds !== 0) {
    args.push(Duration(expiresAfterSeconds * 1e9));
  } else {
    args.push("0");
  }

  const createStmtDiag = {
    sql: `SELECT crdb_internal.request_statement_bundle($1, $2, $3::INTERVAL, $4::INTERVAL) as req_resp`,
    arguments: args,
  };

  const req: SqlExecutionRequest = {
    execute: true,
    statements: [createStmtDiag],
  };

  return checkExistingDiagRequest(stmtFingerprint).then(_ => {
    return executeInternalSql<InsertStmtDiagnosticResponse>(req).then(res => {
      // If request succeeded but query failed, throw error (caught by saga/cacheDataReducer).
      if (res.error) {
        throw res.error;
      }

      if (
        res.execution?.txn_results[0]?.rows?.length === 0 ||
        res.execution?.txn_results[0]?.rows[0]["req_resp"] === false
      ) {
        throw new Error("Failed to insert statement diagnostics request");
      }

      return res.execution.txn_results[0].rows[0];
    });
  });
}

function checkExistingDiagRequest(stmtFingerprint: string): Promise<void> {
  // Query to check if there's already a pending request for this fingerprint.
  const checkPendingStmtDiag = {
    sql: `SELECT count(1) FROM system.statement_diagnostics_requests
        WHERE
          completed = false AND
          statement_fingerprint = $1 AND
          (expires_at IS NULL OR expires_at > now())`,
    arguments: [stmtFingerprint],
  };

  const req: SqlExecutionRequest = {
    execute: true,
    statements: [checkPendingStmtDiag],
  };

  return executeInternalSql<CheckPendingStmtDiagnosticRow>(req).then(res => {
    // If request succeeded but query failed, throw error (caught by saga/cacheDataReducer).
    if (res.error) {
      throw res.error;
    }

    if (res.execution?.txn_results[0]?.rows?.length === 0) {
      throw new Error("Failed to check pending statement diagnostics");
    }

    if (res.execution.txn_results[0].rows[0].count > 0) {
      throw new Error(
        "A pending request for the requested fingerprint already exists. Cancel the existing request first and try again.",
      );
    }
  });
}

export type CancelStmtDiagnosticRequest = {
  requestId: string;
};

export type CancelStmtDiagnosticResponse = {
  stmt_diag_req_id: string;
};

export function cancelStatementDiagnosticsReport({
  requestId,
}: CancelStmtDiagnosticRequest): Promise<CancelStmtDiagnosticResponse> {
  const query = `UPDATE system.statement_diagnostics_requests 
SET expires_at = '1970-01-01' 
WHERE completed = false 
AND id = $1::INT8 
AND (expires_at IS NULL OR expires_at > now()) RETURNING id as stmt_diag_req_id`;
  const req: SqlExecutionRequest = {
    execute: true,
    statements: [
      {
        sql: query,
        arguments: [requestId],
      },
    ],
  };

  return executeInternalSql<CancelStmtDiagnosticResponse>(req).then(res => {
    // If request succeeded but query failed, throw error (caught by saga/cacheDataReducer).
    if (res.error) {
      throw res.error;
    }

    if (!res.execution?.txn_results?.length) {
      throw new Error(
        "cancelStatementDiagnosticsReport - unexpected zero txn_results",
      );
    }

    if (res.execution.txn_results[0].rows?.length === 0) {
      throw new Error(
        `No pending request found for the request id: ${requestId}`,
      );
    }

    return res.execution.txn_results[0].rows[0];
  });
}
