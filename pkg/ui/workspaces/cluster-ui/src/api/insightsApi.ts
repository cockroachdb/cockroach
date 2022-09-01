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
  executeSql,
  SqlExecutionRequest,
  SqlExecutionResponse,
} from "./sqlApi";
import {
  InsightExecEnum,
  InsightNameEnum,
  StatementInsightEvent,
  TransactionInsightEvent,
  TransactionInsightEventDetails,
} from "src/insights";
import moment from "moment";

const apiAppName = "$ api-v2-sql";

export type TransactionInsightEventState = Omit<
  TransactionInsightEvent,
  "insights"
> & {
  insightName: string;
};

export type TransactionInsightEventsResponse = TransactionInsightEventState[];

type InsightQuery<ResponseColumnType, State> = {
  name: InsightNameEnum;
  query: string;
  toState: (response: SqlExecutionResponse<ResponseColumnType>) => State;
};

type TransactionContentionResponseColumns = {
  blocking_txn_id: string;
  blocking_txn_fingerprint_id: string;
  blocking_queries: string[];
  collection_ts: string;
  contention_duration: string;
  threshold: string;
  app_name: string;
};

function transactionContentionResultsToEventState(
  response: SqlExecutionResponse<TransactionContentionResponseColumns>,
): TransactionInsightEventState[] {
  if (!response.execution.txn_results[0].rows) {
    // No data.
    return [];
  }

  return response.execution.txn_results[0].rows.map(row => ({
    transactionID: row.blocking_txn_id,
    fingerprintID: row.blocking_txn_fingerprint_id,
    queries: row.blocking_queries,
    startTime: moment(row.collection_ts),
    contentionDuration: moment.duration(row.contention_duration),
    contentionThreshold: moment.duration(row.threshold).asMilliseconds(),
    application: row.app_name,
    insightName: highContentionQuery.name,
    execType: InsightExecEnum.TRANSACTION,
  }));
}

const highContentionQuery: InsightQuery<
  TransactionContentionResponseColumns,
  TransactionInsightEventsResponse
> = {
  name: InsightNameEnum.highContention,
  query: `SELECT *
          FROM (SELECT blocking_txn_id,
                       encode(blocking_txn_fingerprint_id, 'hex') AS blocking_txn_fingerprint_id,
                       blocking_queries,
                       collection_ts,
                       contention_duration,
                       app_name,
                       row_number()                                  over (
    PARTITION BY
    blocking_txn_fingerprint_id
    ORDER BY collection_ts DESC ) AS rank, threshold
                FROM (SELECT "sql.insights.latency_threshold"::INTERVAL as threshold
                      FROM [SHOW CLUSTER SETTING sql.insights.latency_threshold]),
                     crdb_internal.transaction_contention_events AS tce
                       JOIN (SELECT transaction_fingerprint_id,
                                    app_name,
                                    array_agg(metadata ->> 'query') AS blocking_queries
                             FROM crdb_internal.statement_statistics
                             GROUP BY transaction_fingerprint_id,
                                      app_name) AS bqs
                            ON bqs.transaction_fingerprint_id = tce.blocking_txn_fingerprint_id
                WHERE contention_duration > threshold AND app_name != '${apiAppName}')
          WHERE rank = 1`,
  toState: transactionContentionResultsToEventState,
};

// getTransactionInsightEventState is currently hardcoded to use the High Contention insight type
// for transaction contention events.
export function getTransactionInsightEventState(): Promise<TransactionInsightEventsResponse> {
  const request: SqlExecutionRequest = {
    statements: [
      {
        sql: `${highContentionQuery.query}`,
      },
    ],
    execute: true,
  };
  return executeSql<TransactionContentionResponseColumns>(request).then(
    result => {
      return highContentionQuery.toState(result);
    },
  );
}

// Details.

export type TransactionInsightEventDetailsState = Omit<
  TransactionInsightEventDetails,
  "insights"
> & {
  insightName: string;
};
export type TransactionInsightEventDetailsResponse =
  TransactionInsightEventDetailsState[];
export type TransactionInsightEventDetailsRequest = { id: string };

type TransactionContentionDetailsResponseColumns = {
  blocking_txn_id: string;
  blocking_queries: string[];
  collection_ts: string;
  contention_duration: string;
  threshold: string;
  app_name: string;
  blocking_txn_fingerprint_id: string;
  waiting_txn_id: string;
  waiting_txn_fingerprint_id: string;
  waiting_queries: string[];
  schema_name: string;
  database_name: string;
  table_name: string;
  index_name: string;
  key: string;
};

function transactionContentionDetailsResultsToEventState(
  response: SqlExecutionResponse<TransactionContentionDetailsResponseColumns>,
): TransactionInsightEventDetailsState[] {
  if (!response.execution.txn_results[0].rows) {
    // No data.
    return [];
  }
  return response.execution.txn_results[0].rows.map(row => ({
    executionID: row.blocking_txn_id,
    queries: row.blocking_queries,
    startTime: moment(row.collection_ts),
    elapsedTime: moment.duration(row.contention_duration).asMilliseconds(),
    contentionThreshold: moment.duration(row.threshold).asMilliseconds(),
    application: row.app_name,
    fingerprintID: row.blocking_txn_fingerprint_id,
    waitingExecutionID: row.waiting_txn_id,
    waitingFingerprintID: row.waiting_txn_fingerprint_id,
    waitingQueries: row.waiting_queries,
    schemaName: row.schema_name,
    databaseName: row.database_name,
    tableName: row.table_name,
    indexName: row.index_name,
    contendedKey: row.key,
    insightName: highContentionQuery.name,
    execType: InsightExecEnum.TRANSACTION,
  }));
}

const highContentionDetailsQuery = (
  id: string,
): InsightQuery<
  TransactionContentionDetailsResponseColumns,
  TransactionInsightEventDetailsResponse
> => {
  return {
    name: InsightNameEnum.highContention,
    query: `SELECT collection_ts,
                   blocking_txn_id,
                   encode(blocking_txn_fingerprint_id, 'hex') AS blocking_txn_fingerprint_id,
                   waiting_txn_id,
                   encode(waiting_txn_fingerprint_id, 'hex')  AS waiting_txn_fingerprint_id,
                   contention_duration,
                   crdb_internal.pretty_key(contending_key, 0) as key, 
  database_name, 
  schema_name, 
  table_name, 
  index_name,
  app_name, 
  blocking_queries, 
  waiting_queries,
  threshold
            FROM
  (SELECT "sql.insights.latency_threshold"::INTERVAL as threshold FROM [SHOW CLUSTER SETTING sql.insights.latency_threshold]),
  crdb_internal.transaction_contention_events AS tce 
  LEFT OUTER JOIN crdb_internal.ranges AS ranges ON tce.contending_key BETWEEN ranges.start_key 
  AND ranges.end_key 
  JOIN (
    SELECT 
      transaction_fingerprint_id,
      array_agg(metadata ->> 'query') AS waiting_queries 
    FROM 
      crdb_internal.statement_statistics 
    GROUP BY 
      transaction_fingerprint_id
  ) AS wqs ON wqs.transaction_fingerprint_id = tce.waiting_txn_fingerprint_id 
  JOIN (
    SELECT 
      transaction_fingerprint_id,
      app_name,
      array_agg(metadata ->> 'query') AS blocking_queries 
    FROM 
      crdb_internal.statement_statistics 
    GROUP BY 
      transaction_fingerprint_id,
      app_name
  ) AS bqs ON bqs.transaction_fingerprint_id = tce.blocking_txn_fingerprint_id
            WHERE blocking_txn_id = '${id}'`,
    toState: transactionContentionDetailsResultsToEventState,
  };
};

// getTransactionInsightEventState is currently hardcoded to use the High Contention insight type
// for transaction contention events.
export function getTransactionInsightEventDetailsState(
  req: TransactionInsightEventDetailsRequest,
): Promise<TransactionInsightEventDetailsResponse> {
  const detailsQuery = highContentionDetailsQuery(req.id);
  const request: SqlExecutionRequest = {
    statements: [
      {
        sql: `${detailsQuery.query}`,
      },
    ],
    execute: true,
  };
  return executeSql<TransactionContentionDetailsResponseColumns>(request).then(
    result => {
      return detailsQuery.toState(result);
    },
  );
}

type ExecutionInsightsResponseRow = {
  session_id: string;
  txn_id: string;
  txn_fingerprint_id: string; // hex string
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
  last_retry_reason?: string;
  problems: string[];
  index_recommendations: string[];
};

export type StatementInsights = StatementInsightEvent[];

function getStatementInsightsFromClusterExecutionInsightsResponse(
  response: SqlExecutionResponse<ExecutionInsightsResponseRow>,
): StatementInsights {
  if (!response.execution.txn_results[0].rows) {
    // No data.
    return [];
  }

  return response.execution.txn_results[0].rows.map(row => {
    const start = moment.utc(row.start_time);
    const end = moment.utc(row.end_time);
    return {
      transactionID: row.txn_id,
      transactionFingerprintID: row.txn_fingerprint_id,
      query: row.query,
      startTime: start,
      endTime: end,
      databaseName: row.database_name,
      elapsedTimeMillis: end.diff(start, "milliseconds"),
      application: row.app_name,
      username: row.user_name,
      statementID: row.stmt_id,
      statementFingerprintID: row.stmt_fingerprint_id,
      sessionID: row.session_id,
      isFullScan: row.full_scan,
      rowsRead: row.rows_read,
      rowsWritten: row.rows_written,
      priority: row.priority,
      retries: row.retries,
      lastRetryReason: row.last_retry_reason,
      timeSpentWaiting: row.contention ? moment.duration(row.contention) : null,
      problems: row.problems,
      indexRecommendations: row.index_recommendations,
      insights: null,
    };
  });
}

const statementInsightsQuery: InsightQuery<
  ExecutionInsightsResponseRow,
  StatementInsights
> = {
  name: InsightNameEnum.highContention,
  // We only surface the most recently observed problem for a given statement.
  query: `SELECT *, prettify_statement(non_prettified_query, 108, 2, 1) as query from (
    SELECT
      session_id,
      txn_id,
      encode(txn_fingerprint_id, 'hex')  AS txn_fingerprint_id,
      stmt_id,
      encode(stmt_fingerprint_id, 'hex') AS stmt_fingerprint_id,
      query as non_prettified_query,
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
      last_retry_reason,
      index_recommendations,
      problems,
      row_number()                          OVER (
        PARTITION BY txn_fingerprint_id
        ORDER BY end_time DESC
      ) AS rank
    FROM crdb_internal.cluster_execution_insights
    WHERE array_length(problems, 1) > 0 AND app_name != '${apiAppName}'
  ) WHERE rank = 1
  `,
  toState: getStatementInsightsFromClusterExecutionInsightsResponse,
};

export function getStatementInsightsApi(): Promise<StatementInsights> {
  const request: SqlExecutionRequest = {
    statements: [
      {
        sql: `${statementInsightsQuery.query}`,
      },
    ],
    execute: true,
    max_result_size: 50000, // 50 kib
  };
  return executeSql<ExecutionInsightsResponseRow>(request).then(result => {
    return statementInsightsQuery.toState(result);
  });
}
