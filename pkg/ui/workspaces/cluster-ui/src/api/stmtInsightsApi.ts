// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import {
  SqlApiResponse,
  executeInternalSql,
  formatApiResult,
  LARGE_RESULT_SIZE,
  LONG_TIMEOUT,
  SqlExecutionRequest,
  sqlResultsAreEmpty,
  SqlTxnResult,
} from "./sqlApi";
import {
  ContentionDetails,
  getInsightsFromProblemsAndCauses,
  InsightExecEnum,
  StatementStatus,
  StmtInsightEvent,
} from "src/insights";
import moment from "moment-timezone";
import { INTERNAL_APP_NAME_PREFIX } from "src/util/constants";
import { FixFingerprintHexValue } from "../util";
import { getContentionDetailsApi } from "./contentionApi";

export type StmtInsightsReq = {
  start?: moment.Moment;
  end?: moment.Moment;
  stmtExecutionID?: string;
  stmtFingerprintId?: string;
  useObsService?: boolean;
};

export type StmtInsightsResponseRow = {
  session_id: string;
  txn_id: string;
  txn_fingerprint_id: string; // hex string
  implicit_txn: boolean;
  stmt_id: string;
  stmt_fingerprint_id: string; // hex string
  query: string;
  start_time: string; // Timestamp
  end_time: string; // Timestamp
  full_scan: boolean;
  user_name: string;
  app_name: string;
  database_name: string;
  rows_read: number;
  rows_written: number;
  priority: string;
  retries: number;
  exec_node_ids: number[];
  contention: string; // interval
  contention_events: ContentionDetails[];
  last_retry_reason?: string;
  causes: string[];
  problem: string;
  index_recommendations: string[];
  plan_gist: string;
  cpu_sql_nanos: number;
  error_code: string;
  last_error_redactable: string;
  status: StatementStatus;
};

export type StmtInsightsObsServiceResponseRow = {
  session_id: string;
  transaction_id: string;
  transaction_fingerprint_id: string; // hex string
  implicit_txn: boolean;
  statement_id: string;
  statement_fingerprint_id: string; // hex string
  query: string;
  start_time: string; // Timestamp
  end_time: string; // Timestamp
  full_scan: boolean;
  user_name: string;
  app_name: string;
  database_name: string;
  priority: string;
  retries: number;
  exec_node_ids: number[];
  contention: string; // interval
  last_retry_reason?: string;
  causes: string[];
  problem: string;
  index_recommendations: string[];
  plan_gist: string;
  cpu_sql_nanos: number;
  error_code: string;
  status: StatementStatus;
};

const stmtColumns = `
session_id,
txn_id,
txn_fingerprint_id,
implicit_txn,
stmt_id,
stmt_fingerprint_id,
query,
start_time,
end_time,
full_scan,
user_name,
app_name,
database_name,
rows_read,
rows_written,
priority,
retries,
exec_node_ids,
contention,
last_retry_reason,
causes,
problem,
index_recommendations,
plan_gist,
cpu_sql_nanos,
error_code,
last_error_redactable,
status
`;

// TODO(maryliag): update columns list once we store values for
// rows_read, rows_written and last_error_redactable.
const stmtColumnsObsService = `
session_id,
transaction_id,
transaction_fingerprint_id,
implicit_txn,
statement_id,
statement_fingerprint_id,
query,
start_time,
end_time,
full_scan,
user_name,
app_name,
database_name,
user_priority,
retries,
execution_node_ids,
contention_time,
last_retry_reason,
causes,
problem,
index_recommendations,
plan_gist,
cpu_sql_nanos,
error_code,
status
`;

const stmtInsightsOverviewQuery = (req?: StmtInsightsReq): string => {
  const columns = req.useObsService ? stmtColumnsObsService : stmtColumns;
  const table = req.useObsService
    ? "obsservice.statement_execution_insights"
    : "crdb_internal.cluster_execution_insights";
  const stmtIdColumnName = req.useObsService ? "statement_id" : "stmt_id";
  if (req?.stmtExecutionID) {
    return `SELECT ${columns} FROM ${table} WHERE ${stmtIdColumnName} = '${req.stmtExecutionID}'`;
  }

  const txnIdColumnName = req.useObsService ? "transaction_id" : "txn_id";
  const stmtFingerprintIDColumnName = req.useObsService
    ? "statement_fingerprint_id"
    : "stmt_fingerprint_id";
  let whereClause = `
  WHERE app_name NOT LIKE '${INTERNAL_APP_NAME_PREFIX}%'
  AND problem != 'None'
  AND ${txnIdColumnName} != '00000000-0000-0000-0000-000000000000'`;
  if (req?.start) {
    whereClause =
      whereClause + ` AND start_time >= '${req.start.toISOString()}'`;
  }
  if (req?.end) {
    whereClause = whereClause + ` AND end_time <= '${req.end.toISOString()}'`;
  }
  if (req?.stmtFingerprintId) {
    whereClause =
      whereClause +
      ` AND encode(${stmtFingerprintIDColumnName}, 'hex') = '${req.stmtFingerprintId}'`;
  }

  return `SELECT ${columns} FROM
   (
     SELECT DISTINCT ON (${stmtFingerprintIDColumnName}, problem, causes)
       *
     FROM ${table} ${whereClause}
     ORDER BY ${stmtFingerprintIDColumnName}, problem, causes, end_time DESC
   )`;
};

export const stmtInsightsByTxnExecutionQuery = (id: string): string => `
 SELECT ${stmtColumns}
 FROM crdb_internal.cluster_execution_insights
 WHERE txn_id = '${id}'
`;

export async function getStmtInsightsApi(
  req?: StmtInsightsReq,
): Promise<SqlApiResponse<StmtInsightEvent[]>> {
  const request: SqlExecutionRequest = {
    statements: [
      {
        sql: stmtInsightsOverviewQuery(req),
      },
    ],
    execute: true,
    max_result_size: LARGE_RESULT_SIZE,
    timeout: LONG_TIMEOUT,
  };

  const result = await executeInternalSql<
    StmtInsightsResponseRow | StmtInsightsObsServiceResponseRow
  >(request);

  if (sqlResultsAreEmpty(result)) {
    return formatApiResult<StmtInsightEvent[]>(
      [],
      result.error,
      "retrieving insights information",
    );
  }
  const stmtInsightEvent = formatStmtInsights(
    result.execution?.txn_results[0],
    req.useObsService,
  );
  await addStmtContentionInfoApi(stmtInsightEvent);
  return formatApiResult<StmtInsightEvent[]>(
    stmtInsightEvent,
    result.error,
    "retrieving insights information",
  );
}

async function addStmtContentionInfoApi(
  input: StmtInsightEvent[],
): Promise<void> {
  if (!input || input.length === 0) {
    return;
  }

  for (let i = 0; i < input.length; i++) {
    const event = input[i];
    if (
      event.contentionTime == null ||
      event.contentionTime.asMilliseconds() <= 0
    ) {
      continue;
    }

    const contentionResults = await getContentionDetailsApi({
      waitingTxnID: null,
      waitingStmtID: event.statementExecutionID,
      start: null,
      end: null,
    });

    event.contentionEvents = contentionResults.results;
  }
}

export function formatStmtInsights(
  response: SqlTxnResult<
    StmtInsightsResponseRow | StmtInsightsObsServiceResponseRow
  >,
  useObsService?: boolean,
): StmtInsightEvent[] {
  if (!response?.rows?.length) {
    return [];
  }

  let txnID;
  let txnFingerprintID;
  let stmtID;
  let stmtFingerprintID;
  let rowsRead;
  let rowsWritten;
  let lastErrorRedactable;

  return response.rows.map(
    (row: StmtInsightsResponseRow | StmtInsightsObsServiceResponseRow) => {
      const start = moment.utc(row.start_time);
      const end = moment.utc(row.end_time);
      if (useObsService) {
        const r = row as StmtInsightsObsServiceResponseRow;
        txnID = r.transaction_id;
        txnFingerprintID = r.transaction_fingerprint_id;
        stmtID = r.statement_id;
        stmtFingerprintID = r.statement_fingerprint_id;
        // TODO(maryliag); collect the values for rows read, rows written and last error redactable.
        rowsRead = 0;
        rowsWritten = 0;
        lastErrorRedactable = "";
      } else {
        const r = row as StmtInsightsResponseRow;
        txnID = r.txn_id;
        txnFingerprintID = r.txn_fingerprint_id;
        stmtID = r.stmt_id;
        stmtFingerprintID = r.stmt_fingerprint_id;
        rowsRead = r.rows_read;
        rowsWritten = r.rows_written;
        lastErrorRedactable = r.last_error_redactable;
      }

      return {
        transactionExecutionID: txnID,
        transactionFingerprintID: FixFingerprintHexValue(txnFingerprintID),
        implicitTxn: row.implicit_txn,
        databaseName: row.database_name,
        application: row.app_name,
        username: row.user_name,
        sessionID: row.session_id,
        priority: row.priority,
        retries: row.retries,
        lastRetryReason: row.last_retry_reason,
        query: row.query,
        startTime: start,
        endTime: end,
        elapsedTimeMillis: end.diff(start, "milliseconds"),
        statementExecutionID: stmtID,
        statementFingerprintID: FixFingerprintHexValue(stmtFingerprintID),
        isFullScan: row.full_scan,
        rowsRead: rowsRead,
        rowsWritten: rowsWritten,
        // This is the total stmt contention.
        contentionTime: row.contention ? moment.duration(row.contention) : null,
        indexRecommendations: row.index_recommendations,
        insights: getInsightsFromProblemsAndCauses(
          [row.problem],
          row.causes,
          InsightExecEnum.STATEMENT,
        ),
        planGist: row.plan_gist,
        cpuSQLNanos: row.cpu_sql_nanos,
        errorCode: row.error_code,
        errorMsg: lastErrorRedactable,
        status: row.status,
      } as StmtInsightEvent;
    },
  );
}
