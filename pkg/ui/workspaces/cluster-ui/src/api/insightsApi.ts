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
import { InsightEvent, InsightExecEnum, InsightNameEnum } from "src/insights";
import moment from "moment";

export type InsightEventState = Omit<InsightEvent, "insights"> & {
  insightName: string;
};

export type InsightEventsResponse = InsightEventState[];

type InsightQuery<ResponseColumnType> = {
  name: InsightNameEnum;
  query: string;
  toState: (
    response: SqlExecutionResponse<ResponseColumnType>,
    results: Record<string, InsightEventState>,
  ) => InsightEventState[];
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

const highWaitTimeQuery: InsightQuery<TransactionContentionResponseColumns> = {
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
// for transaction contention events
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
