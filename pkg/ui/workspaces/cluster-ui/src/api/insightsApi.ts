// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { executeSql, SqlExecutionRequest } from "./sqlApi";
import { InsightExecEnum, TransactionInsight } from "../insights";
import moment from "moment";

// TO-DO: Add StatementInsight API

export type TransactionInsightState = Omit<TransactionInsight, "insights">;

type TransactionInsightResponseColumns = {
  blocking_txn_id: string;
  blocking_queries: string;
  collection_ts: string;
  contention_duration: string;
  app_name: string;
};

export type TransactionInsightsResponse = TransactionInsightState[];

export function getTransactionInsightState(): Promise<TransactionInsightsResponse> {
  const request: SqlExecutionRequest = {
    statements: [
      {
        sql: `SELECT
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
        `,
      },
    ],
    execute: true,
  };
  return executeSql<TransactionInsightResponseColumns>(request).then(result => {
    if (!result.execution.txn_results[0].rows) {
      // No data.
      return [];
    }

    const contentionEvents: Record<string, TransactionInsightState> = {};
    result.execution.txn_results[0].rows.forEach(row => {
      const key = row.blocking_txn_id;
      if (!contentionEvents[key]) {
        contentionEvents[key] = {
          executionID: row.blocking_txn_id,
          query: row.blocking_queries,
          startTime: moment(row.collection_ts),
          elapsedTime: moment
            .duration(row.contention_duration)
            .asMilliseconds(),
          application: row.app_name,
          execType: InsightExecEnum.TRANSACTION,
        };
      }
    });

    return Object.values(contentionEvents);
  });
}
