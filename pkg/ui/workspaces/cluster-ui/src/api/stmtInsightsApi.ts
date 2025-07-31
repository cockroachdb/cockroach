// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";

import {
  ContentionDetails,
  InsightExecEnum,
  StatementStatus,
  StmtInsightEvent,
} from "src/insights/types";
import { INTERNAL_APP_NAME_PREFIX } from "src/util/constants";

import { getInsightsFromProblemsAndCauses } from "../insights/utils";
import { FixFingerprintHexValue } from "../util";

import { getContentionDetailsApi } from "./contentionApi";
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

export type StmtInsightsReq = {
  start?: moment.Moment;
  end?: moment.Moment;
  stmtExecutionID?: string;
  stmtFingerprintId?: string;
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
  kv_node_ids: number[];
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
kv_node_ids,
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

const stmtInsightsOverviewQuery = (req?: StmtInsightsReq): string => {
  if (req?.stmtExecutionID) {
    return `SELECT ${stmtColumns} FROM crdb_internal.cluster_execution_insights WHERE stmt_id = '${req.stmtExecutionID}'`;
  }

  let whereClause = `
  WHERE app_name NOT LIKE '${INTERNAL_APP_NAME_PREFIX}%'
  AND problem != 'None'
  AND txn_id != '00000000-0000-0000-0000-000000000000'`;
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
      ` AND encode(stmt_fingerprint_id, 'hex') = '${req.stmtFingerprintId}'`;
  }

  return `SELECT ${stmtColumns} FROM
   (
     SELECT DISTINCT ON (stmt_fingerprint_id, problem, causes)
       *
     FROM crdb_internal.cluster_execution_insights ${whereClause}
     ORDER BY stmt_fingerprint_id, problem, causes, end_time DESC
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

  const result = await executeInternalSql<StmtInsightsResponseRow>(request);

  if (sqlResultsAreEmpty(result)) {
    return formatApiResult<StmtInsightEvent[]>(
      [],
      result.error,
      "retrieving insights information",
    );
  }
  const stmtInsightEvent = formatStmtInsights(result.execution?.txn_results[0]);
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
  response: SqlTxnResult<StmtInsightsResponseRow>,
): StmtInsightEvent[] {
  if (!response?.rows?.length) {
    return [];
  }

  return response.rows.map((row: StmtInsightsResponseRow) => {
    const start = moment.utc(row.start_time);
    const end = moment.utc(row.end_time);
    const r = row as StmtInsightsResponseRow;

    return {
      transactionExecutionID: r.txn_id,
      transactionFingerprintID: FixFingerprintHexValue(r.txn_fingerprint_id),
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
      statementExecutionID: r.stmt_id,
      statementFingerprintID: FixFingerprintHexValue(r.stmt_fingerprint_id),
      isFullScan: row.full_scan,
      rowsRead: row.rows_read,
      rowsWritten: row.rows_written,
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
      errorMsg: row.last_error_redactable,
      status: row.status,
    } as StmtInsightEvent;
  });
}
