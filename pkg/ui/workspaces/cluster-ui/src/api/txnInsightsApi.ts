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
  sqlApiErrorMessage,
  SqlExecutionRequest,
  SqlExecutionResponse,
  sqlResultsAreEmpty,
} from "./sqlApi";
import {
  BlockedContentionDetails,
  dedupInsights,
  StmtInsightEvent,
  getInsightFromCause,
  getInsightsFromProblemsAndCauses,
  InsightExecEnum,
  InsightNameEnum,
  TxnContentionInsightDetails,
  TxnInsightEvent,
} from "src/insights";
import moment from "moment";
import { FixFingerprintHexValue } from "../util";
import { INTERNAL_APP_NAME_PREFIX } from "src/recentExecutions/recentStatementUtils";

export type TxnContentionReq = {
  start?: moment.Moment;
  end?: moment.Moment;
  id?: string;
};

function getTxnContentionWhereClause(
  clause: string,
  filters?: TxnContentionReq,
): string {
  let whereClause = clause;
  if (filters?.start) {
    whereClause =
      whereClause + ` AND collection_ts >= '${filters.start.toISOString()}'`;
  }
  if (filters?.end) {
    whereClause =
      whereClause +
      ` AND (collection_ts + contention_duration) <= '${filters.end.toISOString()}'`;
  }
  return whereClause;
}

export type TxnWithStmtFingerprints = {
  application: string;
  transactionFingerprintID: string;
  queryIDs: string[]; // Statement fingerprint IDs.
};

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

function formatTxnFingerprintsResults(
  response: SqlExecutionResponse<TxnStmtFingerprintsResponseColumns>,
): TxnWithStmtFingerprints[] {
  if (sqlResultsAreEmpty(response)) {
    return [];
  }

  return response.execution.txn_results[0].rows.map(row => ({
    transactionFingerprintID: FixFingerprintHexValue(
      row.transaction_fingerprint_id,
    ),
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

function createStmtFingerprintToQueryMap(
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

// Transaction insight details.

// To get details on a specific transaction contention event:
// 1. Query the crdb_internal.transaction_contention_events table, filtering on the ID specified in the API request.
// 2. Reuse the queries/types defined above to get the waiting and blocking queries.
// After we get the results from these tables, we combine them on the frontend.

// Query 1 types, functions.
export type TransactionContentionEventDetails = Omit<
  TxnContentionInsightDetails,
  "application" | "queries" | "blockingQueries"
>;

// txnContentionDetailsQuery selects information about a specific transaction contention event.
function txnContentionDetailsQuery(filters: TxnContentionReq) {
  const whereClause = getTxnContentionWhereClause(
    ` WHERE waiting_txn_id = '${filters.id}'`,
    filters,
  );
  return `
SELECT DISTINCT
  collection_ts,
  blocking_txn_id,
  encode( blocking_txn_fingerprint_id, 'hex' ) AS blocking_txn_fingerprint_id,
  waiting_txn_id,
  encode( waiting_txn_fingerprint_id, 'hex' ) AS waiting_txn_fingerprint_id,
  contention_duration,
  crdb_internal.pretty_key(contending_key, 0) AS key,
  database_name,
  schema_name,
  table_name,
  index_name,
  threshold
FROM
  (
    SELECT "sql.insights.latency_threshold"::INTERVAL AS threshold
    FROM [SHOW CLUSTER SETTING sql.insights.latency_threshold]
  ),
  crdb_internal.transaction_contention_events AS tce
  JOIN [SELECT database_name,
               schema_name,
               name AS table_name,
               table_id
        FROM
          "".crdb_internal.tables] AS tables ON tce.contending_key BETWEEN crdb_internal.table_span(tables.table_id)[1]
  AND crdb_internal.table_span(tables.table_id)[2]
  LEFT OUTER JOIN [SELECT index_name,
                          descriptor_id,
                          index_id
                   FROM
                     "".crdb_internal.table_indexes] AS indexes ON tce.contending_key BETWEEN crdb_internal.index_span(
  indexes.descriptor_id,
  indexes.index_id
  )[1]
  AND crdb_internal.index_span(
    indexes.descriptor_id,
    indexes.index_id
    )[2]
  ${whereClause}
`;
}

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

type PartialTxnContentionDetails = Omit<
  TxnContentionInsightDetails,
  "application" | "queries"
>;

function formatTxnContentionDetailsResponse(
  response: SqlExecutionResponse<TxnContentionDetailsResponseColumns>,
): PartialTxnContentionDetails {
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
      blockingTxnFingerprintID: FixFingerprintHexValue(
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
  const contentionThreshold = moment.duration(row.threshold).asMilliseconds();
  return {
    transactionExecutionID: row.waiting_txn_id,
    transactionFingerprintID: FixFingerprintHexValue(
      row.waiting_txn_fingerprint_id,
    ),
    startTime: moment(row.collection_ts).utc(),
    totalContentionTimeMs: totalContentionTime,
    blockingContentionDetails: blockingContentionDetails,
    contentionThreshold,
    insightName: InsightNameEnum.highContention,
    execType: InsightExecEnum.TRANSACTION,
    insights: [
      getInsightFromCause(
        InsightNameEnum.highContention,
        InsightExecEnum.TRANSACTION,
        contentionThreshold,
        totalContentionTime,
      ),
    ],
  };
}

// getTransactionInsightEventState is the API function that executes the queries and returns the results.
export async function getTransactionInsightEventDetailsState(
  req: TxnContentionReq,
): Promise<TxnContentionInsightDetails> {
  // Note that any errors encountered fetching these results are caught
  // earlier in the call stack.
  //
  // There are 3 api requests/queries in this process.
  // 1. Get contention insight for the requested transaction.
  // 2. Get the stmt fingerprints for ALL transactions involved in the contention.
  // 3. Get the query strings for ALL statements involved in the transaction.

  // Get contention results for requested transaction.
  const contentionResults =
    await executeInternalSql<TxnContentionDetailsResponseColumns>(
      makeInsightsSqlRequest([txnContentionDetailsQuery(req)]),
    );
  if (contentionResults.error) {
    throw new Error(
      `Error while retrieving contention information: ${sqlApiErrorMessage(
        contentionResults.error.message,
      )}`,
    );
  }
  if (sqlResultsAreEmpty(contentionResults)) {
    return;
  }
  const contentionDetails =
    formatTxnContentionDetailsResponse(contentionResults);

  // Collect all txn fingerprints involved.
  const txnFingerprintIDs: string[] = [];
  contentionDetails.blockingContentionDetails.forEach(x =>
    txnFingerprintIDs.push(x.blockingTxnFingerprintID),
  );
  // Add the waiting txn fingerprint ID.
  txnFingerprintIDs.push(contentionDetails.transactionFingerprintID);

  // Collect all stmt fingerprint ids involved.
  const getStmtFingerprintsResponse =
    await executeInternalSql<TxnStmtFingerprintsResponseColumns>(
      makeInsightsSqlRequest([txnStmtFingerprintsQuery(txnFingerprintIDs)]),
    );
  if (getStmtFingerprintsResponse.error) {
    throw new Error(
      `Error while retrieving statements information: ${sqlApiErrorMessage(
        getStmtFingerprintsResponse.error.message,
      )}`,
    );
  }
  const txnsWithStmtFingerprints = formatTxnFingerprintsResults(
    getStmtFingerprintsResponse,
  );

  const stmtFingerprintIDs = new Set<string>();
  txnsWithStmtFingerprints.forEach(txnFingerprint =>
    txnFingerprint.queryIDs.forEach(id => stmtFingerprintIDs.add(id)),
  );

  const stmtQueriesResponse =
    await executeInternalSql<FingerprintStmtsResponseColumns>(
      makeInsightsSqlRequest([
        fingerprintStmtsQuery(Array.from(stmtFingerprintIDs)),
      ]),
    );
  if (stmtQueriesResponse.error) {
    throw new Error(
      `Error while retrieving statements information: ${sqlApiErrorMessage(
        stmtQueriesResponse.error.message,
      )}`,
    );
  }

  return buildTxnContentionInsightDetails(
    contentionDetails,
    txnsWithStmtFingerprints,
    createStmtFingerprintToQueryMap(stmtQueriesResponse),
  );
}

function buildTxnContentionInsightDetails(
  partialTxnContentionDetails: PartialTxnContentionDetails,
  txnsWithStmtFingerprints: TxnWithStmtFingerprints[],
  stmtFingerprintToQuery: StmtFingerprintToQueryRecord,
): TxnContentionInsightDetails {
  if (
    !partialTxnContentionDetails &&
    !txnsWithStmtFingerprints.length &&
    !stmtFingerprintToQuery.size
  ) {
    return null;
  }

  partialTxnContentionDetails.blockingContentionDetails.forEach(blockedRow => {
    const currBlockedFingerprintStmts = txnsWithStmtFingerprints.find(
      txn =>
        txn.transactionFingerprintID === blockedRow.blockingTxnFingerprintID,
    );

    if (!currBlockedFingerprintStmts) {
      return;
    }

    blockedRow.blockingQueries = currBlockedFingerprintStmts.queryIDs.map(
      id => stmtFingerprintToQuery.get(id) ?? "",
    );
  });

  const waitingTxn = txnsWithStmtFingerprints.find(
    txn =>
      txn.transactionFingerprintID ===
      partialTxnContentionDetails.transactionFingerprintID,
  );

  return {
    ...partialTxnContentionDetails,
    application: waitingTxn.application,
    queries: waitingTxn.queryIDs.map(id => stmtFingerprintToQuery.get(id)),
  };
}

// Statements

type InsightsContentionResponseEvent = {
  blockingTxnID: string;
  durationInMs: number;
  schemaName: string;
  databaseName: string;
  tableName: string;
  indexName: string;
};

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
  contention_events: InsightsContentionResponseEvent[];
  last_retry_reason?: string;
  causes: string[];
  problem: string;
  index_recommendations: string[];
  plan_gist: string;
};

export type FlattenedStmtInsights = StmtInsightEvent[];

// This function collects and groups rows of execution insights into
// a list of transaction insights, which contain any statement insights
// that were returned in the response.
function organizeExecutionInsightsResponseIntoTxns(
  response: SqlExecutionResponse<ExecutionInsightsResponseRow>,
): TxnInsightEvent[] {
  if (!response.execution.txn_results[0].rows) {
    // No data.
    return [];
  }

  // Map of Transaction  exec and fingerprint id -> txn.
  const txnByIDs = new Map<string, TxnInsightEvent>();
  const getTxnKey = (row: ExecutionInsightsResponseRow) =>
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
      transactionExecutionID: row.txn_id,
      transactionFingerprintID: FixFingerprintHexValue(row.txn_fingerprint_id),
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
      statementExecutionID: row.stmt_id,
      statementFingerprintID: FixFingerprintHexValue(row.stmt_fingerprint_id),
      isFullScan: row.full_scan,
      rowsRead: row.rows_read,
      rowsWritten: row.rows_written,
      contentionEvents: row.contention_events,
      contentionTime: row.contention ? moment.duration(row.contention) : null,
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

type InsightQuery<ResponseColumnType, State> = {
  query: string;
  toState: (response: SqlExecutionResponse<ResponseColumnType>) => State;
};

export type QueryFilterFields = {
  id?: string;
  start?: moment.Moment;
  end?: moment.Moment;
};

function workloadInsightsQuery(
  filters?: QueryFilterFields,
): InsightQuery<ExecutionInsightsResponseRow, TxnInsightEvent[]> {
  let whereClause = ` WHERE app_name NOT LIKE '${INTERNAL_APP_NAME_PREFIX}%'`;
  if (filters?.start) {
    whereClause =
      whereClause + ` AND start_time >= '${filters.start.toISOString()}'`;
  }
  if (filters?.end) {
    whereClause =
      whereClause + ` AND end_time <= '${filters.end.toISOString()}'`;
  }
  return {
    // We only surface the most recently observed problem for a given statement.
    // Note that we don't filter by problem != 'None', so that we can get all
    // stmts in the problematic transaction.
    query: `
WITH insightsTable as (
  SELECT 
    * 
  FROM 
    crdb_internal.cluster_execution_insights
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
 `,
    toState: organizeExecutionInsightsResponseIntoTxns,
  };
}

export type ExecutionInsights = TxnInsightEvent[];

export type ExecutionInsightsRequest = Pick<QueryFilterFields, "start" | "end">;

export async function getClusterInsightsApi(
  req?: ExecutionInsightsRequest,
): Promise<ExecutionInsights> {
  const insightsQuery = workloadInsightsQuery(req);
  const request: SqlExecutionRequest = {
    statements: [
      {
        sql: insightsQuery.query,
      },
    ],
    execute: true,
    max_result_size: LARGE_RESULT_SIZE,
    timeout: LONG_TIMEOUT,
  };

  const result = await executeInternalSql<ExecutionInsightsResponseRow>(
    request,
  );
  if (result.error) {
    throw new Error(
      `Error while retrieving insights information: ${sqlApiErrorMessage(
        result.error.message,
      )}`,
    );
  }

  return insightsQuery.toState(result);
}

// We'll replace this and fill out the api properly in the next commit.
export const getTxnInsightEvents = getClusterInsightsApi;
