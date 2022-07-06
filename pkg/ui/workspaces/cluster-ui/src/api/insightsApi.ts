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
} from "src/insights";
import moment from "moment";

export type InsightEventState = Omit<InsightEvent, "insights"> & {
  insightName: string;
};

export type InsightEventsResponse = InsightEventState[];

type InsightQuery<ResponseColumnType, State> = {
  name: InsightNameEnum;
  query: string;
  toState: (
    response: SqlExecutionResponse<ResponseColumnType>,
    results: Record<string, State>,
  ) => State[];
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
  results: Record<string, InsightEventState>,
): InsightEventState[] {
  response.execution.txn_results[0].rows.forEach(row => {
    const key = row.blocking_txn_id;
    if (!results[key]) {
      results[key] = {
        executionID: row.blocking_txn_id,
        queries: row.blocking_queries,
        startTime: moment(row.collection_ts),
        elapsedTime: moment.duration(row.contention_duration).asMilliseconds(),
        application: row.app_name,
        insightName: highWaitTimeQuery.name,
        execType: InsightExecEnum.TRANSACTION,
      };
    }
  });

  return Object.values(results);
}

const highWaitTimeQuery: InsightQuery<
  TransactionContentionResponseColumns,
  InsightEventState
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
      if (!result.execution.txn_results[0].rows) {
        // No data.
        return [];
      }

      const results: Record<string, InsightEventState> = {};
      return highWaitTimeQuery.toState(result, results);
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
  results: Record<string, InsightEventDetailsState>,
): InsightEventDetailsState[] {
  response.execution.txn_results[0].rows.forEach(row => {
    const key = row.blocking_txn_id;
    if (!results[key]) {
      results[key] = {
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
      };
    }
  });

  return Object.values(results);
}

const highWaitTimeDetailsQuery = (
  id: string,
): InsightQuery<
  TransactionContentionDetailsResponseColumns,
  InsightEventDetailsState
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
      if (!result.execution.txn_results[0].rows) {
        // No data.
        return [];
      }
      const results: Record<string, InsightEventDetailsState> = {};
      return detailsQuery.toState(result, results);
    },
  );
}
