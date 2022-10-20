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
  INTERNAL_SQL_API_APP,
  LARGE_RESULT_SIZE,
  LONG_TIMEOUT,
  SqlExecutionRequest,
  SqlExecutionResponse,
} from "./sqlApi";
import {
  BlockedContentionDetails,
  InsightExecEnum,
  InsightNameEnum,
  StatementInsightEvent,
  TransactionInsightEvent,
  TransactionInsightEventDetails,
} from "src/insights";
import moment from "moment";

// Transaction insight events.

// There are three transaction contention event insight queries:
// 1. A query that selects transaction contention events from crdb_internal.cluster_contention_events.
// 2. A query that selects statement fingerprint IDS from crdb_internal.transaction_statistics, filtering on the
// fingerprint IDs recorded in the contention events.
// 3. A query that selects statement queries from crdb_internal.statement_statistics, filtering on the fingerprint IDs
// recorded in the contention event rows.
// After we get the results from these tables, we combine them on the frontend.

// These types describe the final transaction contention event state, as it is stored in Redux.
export type TransactionInsightEventState = Omit<
  TransactionInsightEvent,
  "insights"
> & {
  insightName: string;
};
export type TransactionInsightEventsResponse = TransactionInsightEventState[];

export type TransactionContentionEventState = Omit<
  TransactionInsightEventState,
  "application" | "queries"
>;

export type TransactionContentionEventsResponse =
  TransactionContentionEventState[];

// txnContentionQuery selects all relevant transaction contention events.
const txnContentionQuery = `SELECT *
                            FROM (SELECT waiting_txn_id,
                                         encode(
                                           waiting_txn_fingerprint_id, 'hex'
                                           )                       AS waiting_txn_fingerprint_id,
                                         collection_ts,
                                         total_contention_duration AS contention_duration,
                                         row_number()                 over (
                 PARTITION BY waiting_txn_fingerprint_id
                 ORDER BY
                     collection_ts DESC
                 ) AS rank, threshold
                                  FROM (SELECT "sql.insights.latency_threshold" :: INTERVAL AS threshold
                                        FROM
                                          [SHOW CLUSTER SETTING sql.insights.latency_threshold]),
                                       (SELECT waiting_txn_id,
                                               waiting_txn_fingerprint_id,
                                               max(collection_ts)       AS collection_ts,
                                               sum(contention_duration) AS total_contention_duration
                                        FROM crdb_internal.transaction_contention_events tce
                                        WHERE waiting_txn_fingerprint_id != '\x0000000000000000'
                                        GROUP BY waiting_txn_id, waiting_txn_fingerprint_id),
                                       (SELECT txn_id FROM crdb_internal.cluster_execution_insights)
                                  WHERE total_contention_duration > threshold
                                     OR waiting_txn_id = txn_id)
                            WHERE rank = 1`;

type TransactionContentionResponseColumns = {
  waiting_txn_id: string;
  waiting_txn_fingerprint_id: string;
  collection_ts: string;
  contention_duration: string;
  threshold: string;
};

function transactionContentionResultsToEventState(
  response: SqlExecutionResponse<TransactionContentionResponseColumns>,
): TransactionContentionEventsResponse {
  if (!response.execution.txn_results[0].rows) {
    // No transaction contention events.
    return [];
  }

  return response.execution.txn_results[0].rows.map(row => ({
    transactionID: row.waiting_txn_id,
    fingerprintID: row.waiting_txn_fingerprint_id,
    startTime: moment(row.collection_ts),
    contentionDuration: moment.duration(row.contention_duration),
    contentionThreshold: moment.duration(row.threshold).asMilliseconds(),
    insightName: InsightNameEnum.highContention,
    execType: InsightExecEnum.TRANSACTION,
  }));
}

export type TxnStmtFingerprintEventState = Pick<
  TransactionInsightEventState,
  "application" | "fingerprintID"
> & {
  queryIDs: string[];
};

export type TxnStmtFingerprintEventsResponse = TxnStmtFingerprintEventState[];

type TxnStmtFingerprintsResponseColumns = {
  transaction_fingerprint_id: string;
  query_ids: string[];
  app_name: string;
};

// txnStmtFingerprintsQuery selects all statement fingerprints for each recorded transaction fingerprint.
const txnStmtFingerprintsQuery = (
  txn_fingerprint_ids: string[] | string,
) => `SELECT DISTINCT ON (fingerprint_id) encode(fingerprint_id, 'hex') AS transaction_fingerprint_id,
                                          app_name,
                                          ARRAY(
                                            SELECT jsonb_array_elements_text(metadata -> 'stmtFingerprintIDs')
                                            ) AS query_ids
      FROM crdb_internal.transaction_statistics
      WHERE app_name != '${INTERNAL_SQL_API_APP}'
        AND encode(fingerprint_id, 'hex') = ANY (string_to_array('${txn_fingerprint_ids}'
        , ','))`;

function txnStmtFingerprintsResultsToEventState(
  response: SqlExecutionResponse<TxnStmtFingerprintsResponseColumns>,
): TxnStmtFingerprintEventsResponse {
  if (!response.execution.txn_results[0].rows) {
    // No transaction fingerprint results.
    return [];
  }

  return response.execution.txn_results[0].rows.map(row => ({
    fingerprintID: row.transaction_fingerprint_id,
    queryIDs: row.query_ids,
    application: row.app_name,
  }));
}

type FingerprintStmtsEventState = {
  query: string;
  stmtFingerprintID: string;
};

type FingerprintStmtsEventsResponse = FingerprintStmtsEventState[];

type FingerprintStmtsResponseColumns = {
  statement_fingerprint_id: string;
  query: string;
};

// fingerprintStmtsQuery selects all statement queries for each recorded statement fingerprint.
const fingerprintStmtsQuery = (
  stmt_fingerprint_ids: string[],
) => `SELECT DISTINCT ON (fingerprint_id) encode(fingerprint_id, 'hex') AS statement_fingerprint_id,
                                          prettify_statement(metadata ->> 'query', 108, 2, 1) AS query
      FROM crdb_internal.statement_statistics
      WHERE encode(fingerprint_id, 'hex') = ANY (string_to_array('${stmt_fingerprint_ids}'
        , ','))`;

function fingerprintStmtsResultsToEventState(
  response: SqlExecutionResponse<FingerprintStmtsResponseColumns>,
): FingerprintStmtsEventsResponse {
  if (!response.execution.txn_results[0].rows) {
    // No statement fingerprint results.
    return [];
  }
  return response.execution.txn_results[0].rows.map(row => ({
    stmtFingerprintID: row.statement_fingerprint_id,
    query: row.query,
  }));
}

// getTransactionInsightEventState is the API function that executes the queries and returns the results.
export function getTransactionInsightEventState(): Promise<TransactionInsightEventsResponse> {
  const txnContentionRequest: SqlExecutionRequest = {
    statements: [
      {
        sql: `${txnContentionQuery}`,
      },
    ],
    execute: true,
    max_result_size: LARGE_RESULT_SIZE,
    timeout: LONG_TIMEOUT,
  };
  return executeInternalSql<TransactionContentionResponseColumns>(
    txnContentionRequest,
  ).then(contentionResults => {
    const res = contentionResults.execution.txn_results[0].rows;
    if (!res || res.length < 1) {
      return;
    }
    const txnFingerprintIDs = res.map(row => row.waiting_txn_fingerprint_id);
    const txnFingerprintRequest: SqlExecutionRequest = {
      statements: [
        {
          sql: `${txnStmtFingerprintsQuery(txnFingerprintIDs)}`,
        },
      ],
      execute: true,
      max_result_size: LARGE_RESULT_SIZE,
      timeout: LONG_TIMEOUT,
    };
    return executeInternalSql<TxnStmtFingerprintsResponseColumns>(
      txnFingerprintRequest,
    ).then(txnStmtFingerprintResults => {
      const txnStmtRes =
        txnStmtFingerprintResults.execution.txn_results[0].rows;
      if (!txnStmtRes || txnStmtRes.length < 1) {
        return;
      }
      const stmtFingerprintIDs = txnStmtRes.map(row => row.query_ids);
      const fingerprintStmtsRequest: SqlExecutionRequest = {
        statements: [
          {
            sql: `${fingerprintStmtsQuery(
              [].concat([], ...stmtFingerprintIDs),
            )}`,
          },
        ],
        execute: true,
        max_result_size: LARGE_RESULT_SIZE,
        timeout: LONG_TIMEOUT,
      };
      return executeInternalSql<FingerprintStmtsResponseColumns>(
        fingerprintStmtsRequest,
      ).then(fingerprintStmtResults => {
        return combineTransactionInsightEventState(
          transactionContentionResultsToEventState(contentionResults),
          txnStmtFingerprintsResultsToEventState(txnStmtFingerprintResults),
          fingerprintStmtsResultsToEventState(fingerprintStmtResults),
        );
      });
    });
  });
}

export function combineTransactionInsightEventState(
  txnContentionState: TransactionContentionEventsResponse,
  txnFingerprintState: TxnStmtFingerprintEventsResponse,
  fingerprintStmtState: FingerprintStmtsEventsResponse,
): TransactionInsightEventState[] {
  let res: TransactionInsightEventState[];
  if (txnContentionState && txnFingerprintState && fingerprintStmtState) {
    const queries = txnFingerprintState.map(txnRow => ({
      fingerprintID: txnRow.fingerprintID,
      app_name: txnRow.application,
      queries: txnRow.queryIDs.map(
        stmtID =>
          fingerprintStmtState.find(row => row.stmtFingerprintID === stmtID)
            ?.query,
      ),
    }));
    if (queries) {
      res = txnContentionState.map(row => {
        const qa = queries.find(
          query => query.fingerprintID === row.fingerprintID,
        );
        if (qa) {
          return {
            ...row,
            queries: qa.queries,
            application: qa.app_name,
            insightName: InsightNameEnum.highContention,
            execType: InsightExecEnum.TRANSACTION,
          };
        }
      });
    }
  }
  return res;
}

// Transaction insight details.

// To get details on a specific transaction contention event:
// 1. Query the crdb_internal.transaction_contention_events table, filtering on the ID specified in the API request.
// 2. Reuse the queries/types defined above to get the waiting and blocking queries.
// After we get the results from these tables, we combine them on the frontend.

// These types describe the final event state, as it is stored in Redux
export type TransactionInsightEventDetailsState = Omit<
  TransactionInsightEventDetails,
  "insights"
> & {
  insightName: string;
};
export type TransactionInsightEventDetailsResponse =
  TransactionInsightEventDetailsState;
export type TransactionInsightEventDetailsRequest = { id: string };

// Query 1 types, functions.
export type TransactionContentionEventDetailsState = Omit<
  TransactionInsightEventDetailsState,
  "application" | "queries" | "blockingQueries"
>;

export type TransactionContentionEventDetailsResponse =
  TransactionContentionEventDetailsState;

// txnContentionDetailsQuery selects information about a specific transaction contention event.
const txnContentionDetailsQuery = (id: string) => `SELECT collection_ts,
                                                          blocking_txn_id,
                                                          encode(
                                                            blocking_txn_fingerprint_id, 'hex'
                                                            ) AS blocking_txn_fingerprint_id,
                                                          waiting_txn_id,
                                                          encode(
                                                            waiting_txn_fingerprint_id, 'hex'
                                                            ) AS waiting_txn_fingerprint_id,
                                                          contention_duration,
                                                          crdb_internal.pretty_key(contending_key, 0) AS key,
                                                          database_name,
                                                          schema_name,
                                                          table_name,
                                                          index_name,
                                                          threshold
                                                   FROM (SELECT "sql.insights.latency_threshold" :: INTERVAL AS threshold
                                                         FROM
                                                           [SHOW CLUSTER SETTING sql.insights.latency_threshold]),
                                                        crdb_internal.transaction_contention_events AS tce
                                                          LEFT OUTER JOIN crdb_internal.ranges AS ranges
                                                                          ON tce.contending_key BETWEEN ranges.start_key
                                                                            AND ranges.end_key
                                                   WHERE waiting_txn_id = '${id}'
`;

type TxnContentionDetailsResponseColumns = {
  waiting_txn_id: string;
  waiting_txn_fingerprint_id: string;
  collection_ts: string;
  contention_duration: string;
  threshold: string;
  blocking_txn_id: string;
  blocking_txn_fingerprint_id: string;
  schema_name: string;
  database_name: string;
  table_name: string;
  index_name: string;
  key: string;
};

function transactionContentionDetailsResultsToEventState(
  response: SqlExecutionResponse<TxnContentionDetailsResponseColumns>,
): TransactionContentionEventDetailsResponse {
  const resultsRows = response.execution.txn_results[0].rows;
  if (!resultsRows) {
    // No data.
    return;
  }

  const blockingContentionDetails = new Array<BlockedContentionDetails>(
    resultsRows.length,
  );

  let totalContentionTime = 0;
  resultsRows.forEach((value, idx) => {
    const contentionTimeInMs = moment
      .duration(value.contention_duration)
      .asMilliseconds();
    totalContentionTime += contentionTimeInMs;
    blockingContentionDetails[idx] = {
      blockingExecutionID: value.blocking_txn_id,
      blockingFingerprintID: value.blocking_txn_fingerprint_id,
      blockingQueries: null,
      collectionTimeStamp: moment(value.collection_ts),
      contentionTimeMs: contentionTimeInMs,
      contendedKey: value.key,
      schemaName: value.schema_name,
      databaseName: value.database_name,
      tableName: value.table_name,
      indexName:
        value.index_name && value.index_name !== ""
          ? value.index_name
          : "primary index",
    };
  });

  const row = resultsRows[0];
  return {
    executionID: row.waiting_txn_id,
    fingerprintID: row.waiting_txn_fingerprint_id,
    startTime: moment(row.collection_ts),
    totalContentionTime: totalContentionTime,
    blockingContentionDetails: blockingContentionDetails,
    contentionThreshold: moment.duration(row.threshold).asMilliseconds(),
    insightName: InsightNameEnum.highContention,
    execType: InsightExecEnum.TRANSACTION,
  };
}

// getTransactionInsightEventState is the API function that executes the queries and returns the results.
export function getTransactionInsightEventDetailsState(
  req: TransactionInsightEventDetailsRequest,
): Promise<TransactionInsightEventDetailsResponse> {
  const txnContentionDetailsRequest: SqlExecutionRequest = {
    statements: [
      {
        sql: `${txnContentionDetailsQuery(req.id)}`,
      },
    ],
    execute: true,
    max_result_size: LARGE_RESULT_SIZE,
    timeout: LONG_TIMEOUT,
  };
  return executeInternalSql<TxnContentionDetailsResponseColumns>(
    txnContentionDetailsRequest,
  ).then(contentionResults => {
    const res = contentionResults.execution.txn_results[0].rows;
    if (!res || res.length < 1) {
      return;
    }
    const waitingTxnFingerprintId = res[0].waiting_txn_fingerprint_id;
    const waitingTxnFingerprintRequest: SqlExecutionRequest = {
      statements: [
        {
          sql: `${txnStmtFingerprintsQuery(waitingTxnFingerprintId)}`,
        },
      ],
      execute: true,
      max_result_size: LARGE_RESULT_SIZE,
      timeout: LONG_TIMEOUT,
    };
    return executeInternalSql<TxnStmtFingerprintsResponseColumns>(
      waitingTxnFingerprintRequest,
    ).then(waitingTxnStmtFingerprintIDs => {
      const waitingStmtFingerprintIDs =
        waitingTxnStmtFingerprintIDs.execution.txn_results[0].rows[0].query_ids;
      const waitingFingerprintStmtsRequest: SqlExecutionRequest = {
        statements: [
          {
            sql: `${fingerprintStmtsQuery(waitingStmtFingerprintIDs)}`,
          },
        ],
        execute: true,
        max_result_size: LARGE_RESULT_SIZE,
        timeout: LONG_TIMEOUT,
      };
      return executeInternalSql<FingerprintStmtsResponseColumns>(
        waitingFingerprintStmtsRequest,
      ).then(waitingTxnStmtQueries => {
        let blockingTxnFingerprintId: string[] = [];
        contentionResults.execution.txn_results.forEach(txnResult => {
          blockingTxnFingerprintId = blockingTxnFingerprintId.concat(
            txnResult.rows.map(x => x.blocking_txn_fingerprint_id),
          );
        });

        const blockingTxnFingerprintRequest: SqlExecutionRequest = {
          statements: [
            {
              sql: `${txnStmtFingerprintsQuery(blockingTxnFingerprintId)}`,
            },
          ],
          execute: true,
          max_result_size: LARGE_RESULT_SIZE,
          timeout: LONG_TIMEOUT,
        };
        return executeInternalSql<TxnStmtFingerprintsResponseColumns>(
          blockingTxnFingerprintRequest,
        ).then(blockingTxnStmtFingerprintIDs => {
          let blockingStmtFingerprintIDs: string[] = [];
          blockingTxnStmtFingerprintIDs.execution.txn_results[0].rows.map(
            row => {
              if (row.query_ids && row.query_ids.length > 0) {
                blockingStmtFingerprintIDs = blockingStmtFingerprintIDs.concat(
                  row.query_ids,
                );
              }
            },
          );

          const blockingFingerprintStmtsRequest: SqlExecutionRequest = {
            statements: [
              {
                sql: `${fingerprintStmtsQuery(blockingStmtFingerprintIDs)}`,
              },
            ],
            execute: true,
            max_result_size: LARGE_RESULT_SIZE,
            timeout: LONG_TIMEOUT,
          };
          return executeInternalSql<FingerprintStmtsResponseColumns>(
            blockingFingerprintStmtsRequest,
          ).then(blockingTxnStmtQueries => {
            return combineTransactionInsightEventDetailsState(
              transactionContentionDetailsResultsToEventState(
                contentionResults,
              ),
              txnStmtFingerprintsResultsToEventState(
                waitingTxnStmtFingerprintIDs,
              ),
              txnStmtFingerprintsResultsToEventState(
                blockingTxnStmtFingerprintIDs,
              ),
              fingerprintStmtsResultsToEventState(waitingTxnStmtQueries),
              fingerprintStmtsResultsToEventState(blockingTxnStmtQueries),
            );
          });
        });
      });
    });
  });
}

export function combineTransactionInsightEventDetailsState(
  txnContentionDetailsState: TransactionContentionEventDetailsResponse,
  waitingTxnFingerprintState: TxnStmtFingerprintEventsResponse,
  blockingTxnFingerprintState: TxnStmtFingerprintEventsResponse,
  waitingFingerprintStmtState: FingerprintStmtsEventsResponse,
  blockingFingerprintStmtState: FingerprintStmtsEventsResponse,
): TransactionInsightEventDetailsState {
  let res: TransactionInsightEventDetailsState;
  if (
    txnContentionDetailsState &&
    waitingTxnFingerprintState &&
    blockingTxnFingerprintState &&
    waitingFingerprintStmtState &&
    blockingFingerprintStmtState
  ) {
    txnContentionDetailsState.blockingContentionDetails.forEach(blockedRow => {
      const currBlockedFingerprintStmts = blockingTxnFingerprintState.filter(
        x => x.fingerprintID === blockedRow.blockingFingerprintID,
      );
      if (
        !currBlockedFingerprintStmts ||
        currBlockedFingerprintStmts.length != 1
      ) {
        return;
      }

      blockedRow.blockingQueries = currBlockedFingerprintStmts[0].queryIDs.map(
        id =>
          blockingFingerprintStmtState.find(
            stmt => stmt.stmtFingerprintID === id,
          )?.query,
      );
    });

    res = {
      ...txnContentionDetailsState,
      application: waitingTxnFingerprintState[0].application,
      queries: waitingTxnFingerprintState[0].queryIDs.map(
        id =>
          waitingFingerprintStmtState.find(
            stmt => stmt.stmtFingerprintID === id,
          )?.query,
      ),
    };
  }
  return res;
}

// Statements

type ExecutionInsightsResponseRow = {
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
  last_retry_reason?: string;
  causes: string[];
  problem: string;
  index_recommendations: string[];
  plan_gist: string;
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
      implicitTxn: row.implicit_txn,
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
      causes: row.causes,
      problem: row.problem,
      indexRecommendations: row.index_recommendations,
      insights: null,
      planGist: row.plan_gist,
    };
  });
}

type InsightQuery<ResponseColumnType, State> = {
  name: InsightNameEnum;
  query: string;
  toState: (response: SqlExecutionResponse<ResponseColumnType>) => State;
};

const statementInsightsQuery: InsightQuery<
  ExecutionInsightsResponseRow,
  StatementInsights
> = {
  name: InsightNameEnum.highContention,
  // We only surface the most recently observed problem for a given statement.
  query: `SELECT *, prettify_statement(non_prettified_query, 108, 2, 1) AS query from (
    SELECT
      session_id,
      txn_id,
      encode(txn_fingerprint_id, 'hex')  AS txn_fingerprint_id,
      implicit_txn,
      stmt_id,
      encode(stmt_fingerprint_id, 'hex') AS stmt_fingerprint_id,
      query AS non_prettified_query,
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
      problem,
      causes,
      plan_gist,
      row_number()                          OVER (
        PARTITION BY txn_fingerprint_id
        ORDER BY end_time DESC
      ) AS rank
    FROM crdb_internal.cluster_execution_insights
    WHERE problem != 'None' AND app_name != '${INTERNAL_SQL_API_APP}'
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
    max_result_size: LARGE_RESULT_SIZE,
    timeout: LONG_TIMEOUT,
  };
  return executeInternalSql<ExecutionInsightsResponseRow>(request).then(
    result => {
      return statementInsightsQuery.toState(result);
    },
  );
}
