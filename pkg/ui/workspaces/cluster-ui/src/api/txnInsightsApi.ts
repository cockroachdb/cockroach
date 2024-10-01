// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";

import {
  getInsightsFromProblemsAndCauses,
  InsightExecEnum,
  TransactionStatus,
  TxnInsightEvent,
} from "src/insights";

import { INTERNAL_APP_NAME_PREFIX } from "../util";

import {
  executeInternalSql,
  formatApiResult,
  SqlApiResponse,
  sqlResultsAreEmpty,
} from "./sqlApi";
import { makeInsightsSqlRequest } from "./txnInsightsUtils";

// Txn query string limit for previews in the overview page.
const TXN_QUERY_PREVIEW_MAX = 800;

// Query string limit for txn details.
const QUERY_MAX = 1500;

const TXN_INSIGHTS_TABLE_NAME = "crdb_internal.cluster_txn_execution_insights";

export type TxnWithStmtFingerprints = {
  application: string; // TODO #108051: (xinhaoz) this field seems deprecated.
  transactionFingerprintID: string;
  queryIDs: string[]; // Statement fingerprint IDs.
};

// Exported for testing.
export type TxnStmtFingerprintsResponseColumns = {
  transaction_fingerprint_id: string;
  query_ids: string[]; // Statement Fingerprint IDs.
  app_name: string;
};

export type FingerprintStmtsResponseColumns = {
  statement_fingerprint_id: string;
  query: string;
};

export type TxnInsightsResponseRow = {
  session_id: string;
  txn_id: string;
  txn_fingerprint_id: string; // Hex string
  implicit_txn: boolean;
  query: string;
  start_time: string;
  end_time: string;
  app_name: string;
  user_name: string;
  rows_read: number;
  rows_written: number;
  priority: string;
  retries: number;
  last_retry_reason?: string;
  contention: string; // Duration.
  problems: string[];
  causes: string[];
  stmt_execution_ids: string[];
  cpu_sql_nanos: number;
  last_error_code: string;
  last_error_redactable: string;
  status: TransactionStatus;
};

type TxnQueryFilters = {
  execID?: string;
  fingerprintID?: string;
  start?: moment.Moment;
  end?: moment.Moment;
};

// We only surface the most recently observed problem for a given
// transaction.
export const createTxnInsightsQuery = (filters?: TxnQueryFilters) => {
  const queryLimit = filters.execID ? QUERY_MAX : TXN_QUERY_PREVIEW_MAX;

  const txnColumns = `
session_id,
txn_id,
encode(txn_fingerprint_id, 'hex')  AS txn_fingerprint_id,
implicit_txn,
rpad(query, ${queryLimit}, '') AS query,
start_time,
end_time,
app_name,
user_name,
rows_read,
rows_written,
priority,
retries,
contention,
last_retry_reason,
problems,
causes,
stmt_execution_ids,
cpu_sql_nanos,
last_error_code,
last_error_redactable,
status`;

  if (filters?.execID) {
    return `
SELECT
  ${txnColumns}
FROM ${TXN_INSIGHTS_TABLE_NAME}
WHERE txn_id = '${filters.execID}'
`;
  }

  let whereClause = `
WHERE app_name NOT LIKE '${INTERNAL_APP_NAME_PREFIX}%'
AND txn_id != '00000000-0000-0000-0000-000000000000'`;

  if (filters?.start) {
    whereClause += ` AND start_time >= '${filters.start.toISOString()}'`;
  }

  if (filters?.end) {
    whereClause += ` AND end_time <= '${filters.end.toISOString()}'`;
  }

  if (filters?.fingerprintID) {
    whereClause += ` AND encode(txn_fingerprint_id, 'hex') = '${filters.fingerprintID}'`;
  }

  return `
SELECT ${txnColumns} FROM
  (
    SELECT DISTINCT ON (txn_fingerprint_id, problems, causes)
      *
    FROM
      ${TXN_INSIGHTS_TABLE_NAME}
    ${whereClause}
    ORDER BY txn_fingerprint_id, problems, causes, end_time DESC
  )
`;
};

export function formatTxnInsightsRow(
  row: TxnInsightsResponseRow,
): TxnInsightEvent {
  const startTime = moment.utc(row.start_time);
  const endTime = moment.utc(row.end_time);
  const insights = getInsightsFromProblemsAndCauses(
    row.problems,
    row.causes,
    InsightExecEnum.TRANSACTION,
  );
  return {
    sessionID: row.session_id,
    transactionExecutionID: row.txn_id,
    transactionFingerprintID: row.txn_fingerprint_id,
    implicitTxn: row.implicit_txn,
    query: row.query?.split(" ; ").join("\n") || "",
    startTime,
    endTime,
    elapsedTimeMillis: endTime.diff(startTime, "milliseconds"),
    application: row.app_name,
    username: row.user_name,
    rowsRead: row.rows_read,
    rowsWritten: row.rows_written,
    priority: row.priority,
    retries: row.retries,
    lastRetryReason: row.last_retry_reason,
    contentionTime: moment.duration(row.contention ?? 0),
    insights,
    stmtExecutionIDs: row.stmt_execution_ids,
    cpuSQLNanos: row.cpu_sql_nanos,
    errorCode: row.last_error_code,
    errorMsg: row.last_error_redactable,
    status: row.status,
  };
}

export type TxnInsightsRequest = {
  txnExecutionID?: string;
  txnFingerprintID?: string;
  start?: moment.Moment;
  end?: moment.Moment;
};

export async function getTxnInsightsApi(
  req?: TxnInsightsRequest,
): Promise<SqlApiResponse<TxnInsightEvent[]>> {
  const filters: TxnQueryFilters = {
    start: req?.start,
    end: req?.end,
    execID: req?.txnExecutionID,
    fingerprintID: req?.txnFingerprintID,
  };
  const request = makeInsightsSqlRequest([createTxnInsightsQuery(filters)]);
  const result = await executeInternalSql<TxnInsightsResponseRow>(request);

  if (sqlResultsAreEmpty(result)) {
    return formatApiResult<TxnInsightEvent[]>(
      [],
      result.error,
      "retrieving insights information",
    );
  }

  return formatApiResult<TxnInsightEvent[]>(
    result.execution.txn_results[0].rows.map(formatTxnInsightsRow),
    result.error,
    "retrieving insights information",
  );
}
