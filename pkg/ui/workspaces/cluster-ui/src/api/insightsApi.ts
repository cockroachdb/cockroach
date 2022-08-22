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
  InsightEvent,
  InsightEventDetails,
  InsightExecEnum,
  InsightNameEnum,
  StatementInsightEvent,
} from "src/insights";
import moment from "moment";

export type InsightEventState = Omit<InsightEvent, "insights"> & {
  insightName: string;
};

export type InsightEventsResponse = InsightEventState[];

type InsightQuery<ResponseColumnType, State> = {
  name: InsightNameEnum;
  query: string;
  toState: (response: SqlExecutionResponse<ResponseColumnType>) => State;
};

// The only insight we currently report is "High Wait Time", which is the insight
// associated with each row in the crdb_internal.transaction_contention_events table.
export const HIGH_WAIT_CONTENTION_THRESHOLD = moment.duration(
  200,
  "milliseconds",
);

type TransactionContentionResponseColumns = {
  blocking_txn_id: string;
  blocking_queries: string[];
  collection_ts: string;
  contention_duration: string;
  app_name: string;
};

function transactionContentionResultsToEventState(
  response: SqlExecutionResponse<TransactionContentionResponseColumns>,
): InsightEventState[] {
  if (!response.execution.txn_results[0].rows) {
    // No data.
    return [];
  }

  return response.execution.txn_results[0].rows.map(row => ({
    transactionID: row.blocking_txn_id,
    queries: row.blocking_queries,
    startTime: moment(row.collection_ts),
    elapsedTimeMillis: moment
      .duration(row.contention_duration)
      .asMilliseconds(),
    application: row.app_name,
    insightName: highWaitTimeQuery.name,
    execType: InsightExecEnum.TRANSACTION,
  }));
}

const highWaitTimeQuery: InsightQuery<
  TransactionContentionResponseColumns,
  InsightEventsResponse
> = {
  name: InsightNameEnum.highWaitTime,
  query: `SELECT
            blocking_txn_id,
            blocking_queries,
            collection_ts,
            contention_duration,
            app_name
          FROM
            crdb_internal.transaction_contention_events AS tce
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
          WHERE
            contention_duration > INTERVAL '${HIGH_WAIT_CONTENTION_THRESHOLD.toISOString()}'
  `,
  toState: transactionContentionResultsToEventState,
};

// getInsightEventState is currently hardcoded to use the High Wait Time insight type
// for transaction contention events.
export function getInsightEventState(): Promise<InsightEventsResponse> {
  const request: SqlExecutionRequest = {
    statements: [
      {
        sql: `${highWaitTimeQuery.query}`,
      },
    ],
    execute: true,
  };
  return executeSql<TransactionContentionResponseColumns>(request).then(
    result => {
      return highWaitTimeQuery.toState(result);
    },
  );
}

// Details.

export type InsightEventDetailsState = Omit<InsightEventDetails, "insights"> & {
  insightName: string;
};
export type InsightEventDetailsResponse = InsightEventDetailsState[];
export type InsightEventDetailsRequest = { id: string };

type TransactionContentionDetailsResponseColumns = {
  blocking_txn_id: string;
  blocking_queries: string[];
  collection_ts: string;
  contention_duration: string;
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
): InsightEventDetailsState[] {
  if (!response.execution.txn_results[0].rows) {
    // No data.
    return [];
  }
  return response.execution.txn_results[0].rows.map(row => ({
    executionID: row.blocking_txn_id,
    queries: row.blocking_queries,
    startTime: moment(row.collection_ts),
    elapsedTime: moment.duration(row.contention_duration).asMilliseconds(),
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
    insightName: highWaitTimeQuery.name,
    execType: InsightExecEnum.TRANSACTION,
  }));
}

const highWaitTimeDetailsQuery = (
  id: string,
): InsightQuery<
  TransactionContentionDetailsResponseColumns,
  InsightEventDetailsResponse
> => {
  return {
    name: InsightNameEnum.highWaitTime,
    query: `SELECT
  collection_ts, 
  blocking_txn_id, 
  blocking_txn_fingerprint_id, 
  waiting_txn_id, 
  waiting_txn_fingerprint_id, 
  contention_duration, 
  crdb_internal.pretty_key(contending_key, 0) as key, 
  database_name, 
  schema_name, 
  table_name, 
  index_name,
  app_name, 
  blocking_queries, 
  waiting_queries 
FROM 
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

// getInsightEventState is currently hardcoded to use the High Wait Time insight type
// for transaction contention events.
export function getInsightEventDetailsState(
  req: InsightEventDetailsRequest,
): Promise<InsightEventDetailsResponse> {
  const detailsQuery = highWaitTimeDetailsQuery(req.id);
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
  txn_fingerprint_id: string; // bytes
  stmt_id: string;
  stmt_fingerprint_id: string; // bytes
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
};

export type StatementInsights = Omit<StatementInsightEvent, "insights">[];

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
      queries: [row.query],
      startTime: start,
      endTime: end,
      databaseName: row.database_name,
      elapsedTimeMillis: start.diff(end, "milliseconds"),
      application: row.app_name,
      execType: InsightExecEnum.STATEMENT,
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
    };
  });
}

const statemenInsightsQuery: InsightQuery<
  ExecutionInsightsResponseRow,
  StatementInsights
> = {
  name: InsightNameEnum.highWaitTime,
  // We only surface the most recently observed problem for a given statement.
  query: `SELECT * from (
    SELECT
      session_id,
      txn_id,
      txn_fingerprint_id,
      stmt_id,
      stmt_fingerprint_id,
      query,
      start_time,
      end_time,
      full_scan,
      app_name,
      database_name,
      rows_read,
      rows_written,
      priority,
      retries,
      contention,
      last_retry_reason,
      row_number() OVER (
        PARTITION BY txn_fingerprint_id
        ORDER BY end_time DESC
      ) AS rank
    FROM crdb_internal.cluster_execution_insights
    WHERE array_length(problems, 1) > 0
  ) WHERE rank = 1
  `,
  toState: getStatementInsightsFromClusterExecutionInsightsResponse,
};

export function getStatementInsightsApi(): Promise<StatementInsights> {
  const request: SqlExecutionRequest = {
    statements: [
      {
        sql: `${statemenInsightsQuery.query}`,
      },
    ],
    execute: true,
    max_result_size: 50000, // 50 kib
  };
  return executeSql<ExecutionInsightsResponseRow>(request).then(result => {
    return statemenInsightsQuery.toState(result);
  });
}
