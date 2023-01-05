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
  sqlResultsAreEmpty,
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
import { FixFingerprintHexValue } from "../util";

// Transaction insight events.

// There are three transaction contention event insight queries:
// 1. A query that selects transaction contention events from crdb_internal.transaction_contention_events.
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
const txnContentionQuery = `
SELECT * FROM
(
  SELECT
    waiting_txn_id,
    encode( waiting_txn_fingerprint_id, 'hex' ) AS waiting_txn_fingerprint_id,
    collection_ts,
    total_contention_duration AS contention_duration,
    row_number() OVER ( PARTITION BY waiting_txn_fingerprint_id ORDER BY collection_ts DESC ) AS rank,
    threshold
  FROM
    (
      SELECT "sql.insights.latency_threshold"::INTERVAL AS threshold
      FROM [SHOW CLUSTER SETTING sql.insights.latency_threshold]
    ),
    (
      SELECT
        waiting_txn_id,
        waiting_txn_fingerprint_id,
        max(collection_ts) AS collection_ts,
        sum(contention_duration) AS total_contention_duration
      FROM crdb_internal.transaction_contention_events
      WHERE encode(waiting_txn_fingerprint_id, 'hex') != '0000000000000000'
      GROUP BY waiting_txn_id, waiting_txn_fingerprint_id
    )
  WHERE total_contention_duration > threshold
)
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
  if (sqlResultsAreEmpty(response)) {
    // No transaction contention events.
    return [];
  }

  return response.execution.txn_results[0].rows.map(row => ({
    transactionID: row.waiting_txn_id,
    fingerprintID: FixFingerprintHexValue(row.waiting_txn_fingerprint_id),
    startTime: moment(row.collection_ts).utc(),
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
  query_ids: string[]; // Statement Fingerprint IDs.
  app_name: string;
};

// txnStmtFingerprintsQuery selects all statement fingerprints for each recorded transaction fingerprint.
const txnStmtFingerprintsQuery = (txn_fingerprint_ids: string[]) => `
SELECT
  DISTINCT ON (fingerprint_id) encode(fingerprint_id, 'hex') AS transaction_fingerprint_id,
  app_name,
  ARRAY( SELECT jsonb_array_elements_text(metadata -> 'stmtFingerprintIDs' )) AS query_ids
FROM crdb_internal.transaction_statistics
WHERE app_name != '${INTERNAL_SQL_API_APP}'
  AND encode(fingerprint_id, 'hex') = ANY ARRAY[ ${txn_fingerprint_ids
    .map(id => `'${id}'`)
    .join(",")} ]`;

function txnStmtFingerprintsResultsToEventState(
  response: SqlExecutionResponse<TxnStmtFingerprintsResponseColumns>,
): TxnStmtFingerprintEventsResponse {
  if (sqlResultsAreEmpty(response)) {
    // No transaction fingerprint results.
    return [];
  }

  return response.execution.txn_results[0].rows.map(row => ({
    fingerprintID: FixFingerprintHexValue(row.transaction_fingerprint_id),
    queryIDs: row.query_ids,
    application: row.app_name,
  }));
}

type StmtFingerprintToQueryRecord = Map<
  string, // Key = Stmt fingerprint ID
  string // Value = query string
>;

type FingerprintStmtsResponseColumns = {
  statement_fingerprint_id: string;
  query: string;
};

// fingerprintStmtsQuery selects all statement queries for each recorded statement fingerprint.
const fingerprintStmtsQuery = (stmt_fingerprint_ids: string[]): string => `
SELECT
  DISTINCT ON (fingerprint_id) encode(fingerprint_id, 'hex') AS statement_fingerprint_id,
  prettify_statement(metadata ->> 'query', 108, 1, 1) AS query
FROM crdb_internal.statement_statistics
WHERE encode(fingerprint_id, 'hex') = ANY ARRAY[ ${stmt_fingerprint_ids
  .map(id => `'${id}'`)
  .join(",")} ]`;

function fingerprintStmtsResultsToEventState(
  response: SqlExecutionResponse<FingerprintStmtsResponseColumns>,
): StmtFingerprintToQueryRecord {
  const idToQuery: Map<string, string> = new Map();
  if (sqlResultsAreEmpty(response)) {
    // No statement fingerprint results.
    return idToQuery;
  }
  response.execution.txn_results[0].rows.forEach(row => {
    idToQuery.set(
      FixFingerprintHexValue(row.statement_fingerprint_id),
      row.query,
    );
  });

  return idToQuery;
}

const makeInsightsSqlRequest = (queries: string[]): SqlExecutionRequest => ({
  statements: queries.map(query => ({ sql: query })),
  execute: true,
  max_result_size: LARGE_RESULT_SIZE,
  timeout: LONG_TIMEOUT,
});

// getTransactionInsightEventState is the API function that executes the queries and returns the results.
export async function getTransactionInsightEventState(): Promise<TransactionInsightEventsResponse> {
  // Note that any errors encountered fetching these results are caught
  // earlier in the call stack.

  // Step 1: Get transaction contention events that are over the insights
  // latency threshold.
  const contentionResults =
    await executeInternalSql<TransactionContentionResponseColumns>(
      makeInsightsSqlRequest([txnContentionQuery]),
    );
  if (sqlResultsAreEmpty(contentionResults)) {
    return [];
  }

  // Step 2: Fetch the stmt fingerprints in the contended transactions.
  const txnFingerprintIDs = new Set<string>();
  contentionResults.execution.txn_results[0].rows.forEach(row =>
    txnFingerprintIDs.add(
      FixFingerprintHexValue(row.waiting_txn_fingerprint_id),
    ),
  );

  const txnStmtFingerprintResults =
    await executeInternalSql<TxnStmtFingerprintsResponseColumns>(
      makeInsightsSqlRequest([
        txnStmtFingerprintsQuery(Array.from(txnFingerprintIDs)),
      ]),
    );
  if (sqlResultsAreEmpty(txnStmtFingerprintResults)) {
    return [];
  }

  // Step 3: Get all query strings for statement fingerprints.
  const stmtFingerprintIDs = new Set<string>();
  txnStmtFingerprintResults.execution.txn_results[0].rows.forEach(row => {
    row.query_ids.forEach(id => stmtFingerprintIDs.add(id));
  });
  const fingerprintStmtsRequest = makeInsightsSqlRequest([
    fingerprintStmtsQuery(Array.from(stmtFingerprintIDs)),
  ]);
  const fingerprintStmtResults =
    await executeInternalSql<FingerprintStmtsResponseColumns>(
      fingerprintStmtsRequest,
    );

  return combineTransactionInsightEventState(
    transactionContentionResultsToEventState(contentionResults),
    txnStmtFingerprintsResultsToEventState(txnStmtFingerprintResults),
    fingerprintStmtsResultsToEventState(fingerprintStmtResults),
  );
}

export function combineTransactionInsightEventState(
  txnContentionState: TransactionContentionEventsResponse,
  txnFingerprintState: TxnStmtFingerprintEventsResponse,
  fingerprintToQuery: StmtFingerprintToQueryRecord,
): TransactionInsightEventState[] {
  if (
    !txnContentionState.length ||
    !txnFingerprintState.length ||
    !fingerprintToQuery.size
  ) {
    return [];
  }
  const txnsWithStmtQueries = txnFingerprintState.map(txnRow => ({
    fingerprintID: txnRow.fingerprintID,
    appName: txnRow.application,
    queries: txnRow.queryIDs.map(stmtID => fingerprintToQuery.get(stmtID)),
  }));

  const res = txnContentionState.map(row => {
    const qa = txnsWithStmtQueries.find(
      query => query.fingerprintID === row.fingerprintID,
    );
    if (qa) {
      return {
        ...row,
        queries: qa.queries,
        application: qa.appName,
        insightName: InsightNameEnum.highContention,
        execType: InsightExecEnum.TRANSACTION,
      };
    }
  });

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
const txnContentionDetailsQuery = (id: string) => `
SELECT
  DISTINCT collection_ts,
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
FROM
  (
    SELECT
      "sql.insights.latency_threshold" :: INTERVAL AS threshold
    FROM
      [SHOW CLUSTER SETTING sql.insights.latency_threshold]
  ),
  crdb_internal.transaction_contention_events AS tce
    JOIN [SELECT database_name,
                 schema_name,
                 name AS table_name,
                 table_id
          FROM
            "".crdb_internal.tables] AS tables ON tce.contending_key BETWEEN crdb_internal.table_span(tables.table_id) [1]
    AND crdb_internal.table_span(tables.table_id) [2]
    LEFT OUTER JOIN [SELECT index_name,
                            descriptor_id,
                            index_id
                     FROM
                       "".crdb_internal.table_indexes] AS indexes ON tce.contending_key BETWEEN crdb_internal.index_span(
    indexes.descriptor_id, indexes.index_id
    ) [1]
    AND crdb_internal.index_span(
      indexes.descriptor_id, indexes.index_id
      ) [2]
WHERE
  waiting_txn_id = '${id}'
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
      blockingFingerprintID: FixFingerprintHexValue(
        value.blocking_txn_fingerprint_id,
      ),
      blockingQueries: null,
      collectionTimeStamp: moment(value.collection_ts).utc(),
      contentionTimeMs: contentionTimeInMs,
      contendedKey: value.key,
      schemaName: value.schema_name,
      databaseName: value.database_name,
      tableName: value.table_name,
      indexName:
        value.index_name && value.index_name !== ""
          ? value.index_name
          : "index not found",
    };
  });

  const row = resultsRows[0];
  return {
    executionID: row.waiting_txn_id,
    fingerprintID: FixFingerprintHexValue(row.waiting_txn_fingerprint_id),
    startTime: moment(row.collection_ts).utc(),
    totalContentionTime: totalContentionTime,
    blockingContentionDetails: blockingContentionDetails,
    contentionThreshold: moment.duration(row.threshold).asMilliseconds(),
    insightName: InsightNameEnum.highContention,
    execType: InsightExecEnum.TRANSACTION,
  };
}

// getTransactionInsightEventState is the API function that executes the queries and returns the results.
export async function getTransactionInsightEventDetailsState(
  req: TransactionInsightEventDetailsRequest,
): Promise<TransactionInsightEventDetailsResponse> {
  // Note that any errors encountered fetching these results are caught // earlier in the call stack.
  //
  // There are 3 api requests/queries in this process.
  // 1. Get contention insight for the requested transaction.
  // 2. Get the stmt fingerprints for ALL transactions involved in the contention.
  // 3. Get the query strings for ALL statements involved in the transaction.

  // Get contention results for requested transaction.
  const txnContentionDetailsRequest = makeInsightsSqlRequest([
    txnContentionDetailsQuery(req.id),
  ]);
  const contentionResults =
    await executeInternalSql<TxnContentionDetailsResponseColumns>(
      txnContentionDetailsRequest,
    );
  if (sqlResultsAreEmpty(contentionResults)) {
    return;
  }

  // Collect all txn fingerprints involved.
  const txnFingerprintIDs: string[] = [];
  contentionResults.execution.txn_results.forEach(txnResult =>
    txnResult.rows.forEach(x =>
      txnFingerprintIDs.push(
        FixFingerprintHexValue(x.blocking_txn_fingerprint_id),
      ),
    ),
  );
  // Add the waiting txn fingerprint ID.
  txnFingerprintIDs.push(
    FixFingerprintHexValue(
      contentionResults.execution.txn_results[0].rows[0]
        .waiting_txn_fingerprint_id,
    ),
  );

  // Collect all stmt fingerprint ids involved.
  const getStmtFingerprintsResponse =
    await executeInternalSql<TxnStmtFingerprintsResponseColumns>(
      makeInsightsSqlRequest([txnStmtFingerprintsQuery(txnFingerprintIDs)]),
    );

  const stmtFingerprintIDs = new Set<string>();
  getStmtFingerprintsResponse.execution.txn_results[0].rows.forEach(
    txnFingerprint =>
      txnFingerprint.query_ids.forEach(id => stmtFingerprintIDs.add(id)),
  );
  console.log("ok");
  console.log(txnFingerprintIDs);
  console.log(stmtFingerprintIDs);
  const stmtQueriesResponse =
    await executeInternalSql<FingerprintStmtsResponseColumns>(
      makeInsightsSqlRequest([
        fingerprintStmtsQuery(Array.from(stmtFingerprintIDs)),
      ]),
    );

  return combineTransactionInsightEventDetailsState(
    transactionContentionDetailsResultsToEventState(contentionResults),
    txnStmtFingerprintsResultsToEventState(getStmtFingerprintsResponse),
    fingerprintStmtsResultsToEventState(stmtQueriesResponse),
  );
}

export function combineTransactionInsightEventDetailsState(
  txnContentionDetailsState: TransactionContentionEventDetailsResponse,
  txnsWithStmtFingerprints: TxnStmtFingerprintEventsResponse,
  stmtFingerprintToQuery: StmtFingerprintToQueryRecord,
): TransactionInsightEventDetailsState {
  if (
    !txnContentionDetailsState &&
    !txnsWithStmtFingerprints.length &&
    !stmtFingerprintToQuery.size
  ) {
    return null;
  }

  txnContentionDetailsState.blockingContentionDetails.forEach(blockedRow => {
    const currBlockedFingerprintStmts = txnsWithStmtFingerprints.find(
      txn => txn.fingerprintID === blockedRow.blockingFingerprintID,
    );

    if (!currBlockedFingerprintStmts) {
      return;
    }

    blockedRow.blockingQueries = currBlockedFingerprintStmts.queryIDs.map(
      id => stmtFingerprintToQuery.get(id) ?? "",
    );
  });

  const waitingTxn = txnsWithStmtFingerprints.find(
    txn => txn.fingerprintID === txnContentionDetailsState.fingerprintID,
  );

  const res = {
    ...txnContentionDetailsState,
    application: waitingTxn.application,
    queries: waitingTxn.queryIDs.map(id => stmtFingerprintToQuery.get(id)),
  };

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
      transactionFingerprintID: FixFingerprintHexValue(row.txn_fingerprint_id),
      implicitTxn: row.implicit_txn,
      query: row.query,
      startTime: start,
      endTime: end,
      databaseName: row.database_name,
      elapsedTimeMillis: end.diff(start, "milliseconds"),
      application: row.app_name,
      username: row.user_name,
      statementID: row.stmt_id,
      statementFingerprintID: FixFingerprintHexValue(row.stmt_fingerprint_id),
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
  query: `SELECT *, prettify_statement(non_prettified_query, 108, 1, 1) AS query from (
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
      row_number()    OVER (
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
        sql: statementInsightsQuery.query,
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
