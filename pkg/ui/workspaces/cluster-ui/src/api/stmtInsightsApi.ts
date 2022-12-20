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
  executeInternalSql,
  LARGE_RESULT_SIZE,
  LONG_TIMEOUT,
  SqlExecutionRequest,
  SqlExecutionResponse,
  sqlResultsAreEmpty,
} from "./sqlApi";
import {
  dedupInsights,
  getInsightsFromProblemsAndCauses,
  InsightExecEnum,
  TxnInsightEvent,
} from "src/insights";
import moment from "moment";
import { INTERNAL_APP_NAME_PREFIX } from "src/recentExecutions/recentStatementUtils";
import { FixFingerprintHexValue } from "../util";

export type StmtInsightsReq = {
  start?: moment.Moment;
  end?: moment.Moment;
};

type InsightsContentionResponseEvent = {
  blockingTxnID: string;
  durationInMs: number;
  schemaName: string;
  databaseName: string;
  tableName: string;
  indexName: string;
};

type StmtInsightsResponseRow = {
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
  contention_events: InsightsContentionResponseEvent[];
  last_retry_reason?: string;
  causes: string[];
  problem: string;
  index_recommendations: string[];
  plan_gist: string;
};

// This function collects and groups rows of execution insights into
// a list of transaction insights, which contain any statement insights
// that were returned in the response.
function organizeExecutionInsightsResponseIntoTxns(
  response: SqlExecutionResponse<StmtInsightsResponseRow>,
): TxnInsightEvent[] {
  if (sqlResultsAreEmpty(response)) {
    // No data.
    return [];
  }

  // Map of Transaction  exec and fingerprint id -> txn.
  const txnByIDs = new Map<string, TxnInsightEvent>();
  const getTxnKey = (row: StmtInsightsResponseRow) =>
    row.txn_id.concat(row.txn_fingerprint_id);

  response.execution.txn_results[0].rows.forEach(row => {
    const rowKey = getTxnKey(row);
    let txnInsight: TxnInsightEvent = txnByIDs.get(rowKey);

    if (!txnInsight) {
      txnInsight = {
        transactionExecutionID: row.txn_id,
        transactionFingerprintID: FixFingerprintHexValue(
          row.txn_fingerprint_id,
        ),
        implicitTxn: row.implicit_txn,
        databaseName: row.database_name,
        application: row.app_name,
        username: row.user_name,
        sessionID: row.session_id,
        priority: row.priority,
        retries: row.retries,
        lastRetryReason: row.last_retry_reason,
        statementInsights: [],
        insights: [],
        queries: [],
      };
      txnByIDs.set(rowKey, txnInsight);
    }

    const start = moment.utc(row.start_time);
    const end = moment.utc(row.end_time);
    const stmtInsight = {
      query: row.query,
      startTime: start,
      endTime: end,
      elapsedTimeMillis: end.diff(start, "milliseconds"),
      statementExecutionID: row.stmt_id,
      statementFingerprintID: FixFingerprintHexValue(row.stmt_fingerprint_id),
      isFullScan: row.full_scan,
      rowsRead: row.rows_read,
      rowsWritten: row.rows_written,
      contentionEvents: row.contention_events,
      totalContentionTime: row.contention
        ? moment.duration(row.contention)
        : null,
      causes: row.causes,
      problem: row.problem,
      indexRecommendations: row.index_recommendations,
      insights: getInsightsFromProblemsAndCauses(
        row.problem,
        row.causes,
        InsightExecEnum.STATEMENT,
      ),
      planGist: row.plan_gist,
    };

    txnInsight.queries.push(stmtInsight.query);
    txnInsight.statementInsights.push(stmtInsight);

    // Bubble up stmt insights to txn level.
    txnInsight.insights = txnInsight.insights.concat(
      getInsightsFromProblemsAndCauses(
        row.problem,
        row.causes,
        InsightExecEnum.TRANSACTION,
      ),
    );
  });

  txnByIDs.forEach(txn => {
    // De-duplicate top-level txn insights.
    txn.insights = dedupInsights(txn.insights);

    // Sort stmt insights for each txn by start time.
    txn.statementInsights.sort((a, b) => {
      if (a.startTime.isBefore(b.startTime)) return -1;
      else if (a.startTime.isAfter(b.startTime)) return 1;
      return 0;
    });
  });

  return Array.from(txnByIDs.values());
}

// We only surface the most recently observed problem for a given statement.
// Note that we don't filter by problem != 'None', so that we can get all
// stmts in the problematic transaction.
const stmtInsightsQuery = (filters: StmtInsightsReq) => {
  let whereClause = ` WHERE app_name NOT LIKE '${INTERNAL_APP_NAME_PREFIX}%'`;
  if (filters?.start) {
    whereClause =
      whereClause + ` AND start_time >= '${filters.start.toISOString()}'`;
  }
  if (filters?.end) {
    whereClause =
      whereClause + ` AND end_time <= '${filters.end.toISOString()}'`;
  }

  return `
WITH insightsTable as (
  SELECT * FROM crdb_internal.cluster_execution_insights
  ${whereClause}
) 
SELECT
  session_id,
  insights.txn_id as txn_id,
  encode(txn_fingerprint_id, 'hex')  AS txn_fingerprint_id,
  implicit_txn,
  stmt_id,
  encode(stmt_fingerprint_id, 'hex') AS stmt_fingerprint_id,
  prettify_statement(query, 108, 1, 1) AS query,
  start_time,
  end_time,
  full_scan,
  app_name,
  database_name,
  user_name,
  rows_read,
  rows_written,
  priority,
  retries,
  contention,
  contention_events,
  last_retry_reason,
  index_recommendations,
  problem,
  causes,
  plan_gist
FROM
  (
    SELECT
     txn_id,
     row_number() OVER ( PARTITION BY txn_fingerprint_id ORDER BY end_time DESC ) as rank
    FROM insightsTable
  ) as latestTxns
JOIN insightsTable AS insights
ON latestTxns.txn_id = insights.txn_id
WHERE latestTxns.rank = 1
 `;
};

export type ExecutionInsights = TxnInsightEvent[];
export function getStmtInsightsApi(
  req: StmtInsightsReq,
): Promise<ExecutionInsights> {
  const request: SqlExecutionRequest = {
    statements: [
      {
        sql: stmtInsightsQuery(req),
      },
    ],
    execute: true,
    max_result_size: LARGE_RESULT_SIZE,
    timeout: LONG_TIMEOUT,
  };
  return executeInternalSql<StmtInsightsResponseRow>(request).then(result => {
    return organizeExecutionInsightsResponseIntoTxns(result);
  });
}
