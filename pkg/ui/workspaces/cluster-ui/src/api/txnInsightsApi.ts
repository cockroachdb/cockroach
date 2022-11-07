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
  ExecutionInsightCountEvent,
  getInsightsFromProblemsAndCauses,
  InsightExecEnum,
  InsightNameEnum,
  InsightType,
  TxnContentionInsightDetails,
  TxnInsightDetails,
  TxnInsightEvent,
} from "src/insights";
import moment from "moment";
import { FixFingerprintHexValue } from "../util";
import {
  formatStmtInsights,
  stmtInsightsByTxnExecutionQuery,
  StmtInsightsResponseRow,
} from "./stmtInsightsApi";
import { INTERNAL_APP_NAME_PREFIX } from "src/recentExecutions/recentStatementUtils";

export const TXN_QUERY_PREVIEW_MAX = 800;
export const QUERY_MAX = 1500;
export const TXN_INSIGHTS_TABLE_NAME =
  "crdb_internal.cluster_txn_execution_insights";

const makeInsightsSqlRequest = (
  queries: Array<string | null>,
): SqlExecutionRequest => ({
  statements: queries.filter(q => q).map(query => ({ sql: query })),
  execute: true,
  max_result_size: LARGE_RESULT_SIZE,
  timeout: LONG_TIMEOUT,
});

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

// txnStmtFingerprintsQuery selects all statement fingerprints for each
// requested transaction fingerprint.
const txnStmtFingerprintsQuery = (txnFingerprintIDs: string[]) => `
SELECT
  DISTINCT ON (fingerprint_id) encode(fingerprint_id, 'hex') AS transaction_fingerprint_id,
  app_name,
  ARRAY( SELECT jsonb_array_elements_text(metadata -> 'stmtFingerprintIDs' )) AS query_ids
FROM crdb_internal.transaction_statistics
WHERE app_name != '${INTERNAL_SQL_API_APP}'
  AND encode(fingerprint_id, 'hex') = 
      ANY ARRAY[ ${txnFingerprintIDs.map(id => `'${id}'`).join(",")} ]`;

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

// Query to select all statement queries for each requested statement
// fingerprint.
const fingerprintStmtsQuery = (stmtFingerprintIDs: string[]): string => `
SELECT
  DISTINCT ON (fingerprint_id) encode(fingerprint_id, 'hex') AS statement_fingerprint_id,
  prettify_statement(metadata ->> 'query', 108, 1, 1) AS query
FROM crdb_internal.statement_statistics
WHERE encode(fingerprint_id, 'hex') =
      ANY ARRAY[ ${stmtFingerprintIDs.map(id => `'${id}'`).join(",")} ]`;

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

// txnContentionDetailsQuery selects information about a specific transaction contention event.
function txnContentionDetailsQuery(filters: TxnContentionDetailsRequest) {
  let whereClause = ` WHERE waiting_txn_id = '${filters.txnExecutionID}'`;
  if (filters?.start) {
    whereClause =
      whereClause + ` AND collection_ts >= '${filters.start.toISOString()}'`;
  }
  if (filters?.end) {
    whereClause =
      whereClause +
      ` AND (collection_ts + contention_duration) <= '${filters.end.toISOString()}'`;
  }
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
  index_name
FROM
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

  resultsRows.forEach((value, idx) => {
    const contentionTimeInMs = moment
      .duration(value.contention_duration)
      .asMilliseconds();
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
  return {
    transactionExecutionID: row.waiting_txn_id,
    transactionFingerprintID: FixFingerprintHexValue(
      row.waiting_txn_fingerprint_id,
    ),
    blockingContentionDetails: blockingContentionDetails,
    insightName: InsightNameEnum.highContention,
    execType: InsightExecEnum.TRANSACTION,
  };
}

export type TxnContentionDetailsRequest = {
  txnExecutionID?: string;
  start?: moment.Moment;
  end?: moment.Moment;
};

export async function getTxnInsightsContentionDetailsApi(
  req: TxnInsightDetailsRequest,
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
      makeInsightsSqlRequest([
        txnContentionDetailsQuery({
          txnExecutionID: req.txnExecutionID,
          start: req.start,
          end: req.end,
        }),
      ]),
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

  // Collect all blocking txn fingerprints involved.
  const txnFingerprintIDs: string[] = [];
  contentionDetails.blockingContentionDetails.forEach(x =>
    txnFingerprintIDs.push(x.blockingTxnFingerprintID),
  );

  // Request all blocking stmt fingerprint ids involved.
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

  // Request query string from stmt fingerprint ids.
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
    application: waitingTxn?.application,
  };
}

type TxnInsightsResponseRow = {
  session_id: string;
  txn_id: string;
  txn_fingerprint_id: string; // Hex string
  implicit_txn: boolean;
  query: string;
  start_time: string;
  end_time: string;
  app_name: string;
  user_name: string;
  rows_read: number;
  rows_written: number;
  priority: string;
  retries: number;
  last_retry_reason?: string;
  contention: string; // Duration.
  problems: string[];
  causes: string[];
  stmt_execution_ids: string[];
  cpu_sql_nanos: number;
};

type TxnQueryFilters = {
  execID?: string;
  fingerprintID?: string;
  start?: moment.Moment;
  end?: moment.Moment;
};

// We only surface the most recently observed problem for a given
// transaction.
const createTxnInsightsQuery = (filters?: TxnQueryFilters) => {
  const queryLimit = filters.execID ? QUERY_MAX : TXN_QUERY_PREVIEW_MAX;

  const txnColumns = `
session_id,
txn_id,
encode(txn_fingerprint_id, 'hex')  AS txn_fingerprint_id,
implicit_txn,
rpad(query, ${queryLimit}, '') AS query,
start_time,
end_time,
app_name,
user_name,
rows_read,
rows_written,
priority,
retries,
contention,
last_retry_reason,
problems,
causes,
stmt_execution_ids,
cpu_sql_nanos`;

  if (filters?.execID) {
    return `
SELECT
  ${txnColumns}
FROM ${TXN_INSIGHTS_TABLE_NAME}
WHERE txn_id = '${filters.execID}'
`;
  }

  let whereClause = `
WHERE app_name NOT LIKE '${INTERNAL_APP_NAME_PREFIX}%'
AND txn_id != '00000000-0000-0000-0000-000000000000'`;

  if (filters?.start) {
    whereClause += ` AND start_time >= '${filters.start.toISOString()}'`;
  }

  if (filters?.end) {
    whereClause += ` AND end_time <= '${filters.end.toISOString()}'`;
  }

  if (filters?.fingerprintID) {
    whereClause += ` AND encode(txn_fingerprint_id, 'hex') = '${filters.fingerprintID}'`;
  }

  return `
SELECT ${txnColumns} FROM
  (
    SELECT DISTINCT ON (txn_fingerprint_id, problems, causes)
      *
    FROM
      ${TXN_INSIGHTS_TABLE_NAME}
    ${whereClause}
    ORDER BY txn_fingerprint_id, problems, causes, end_time DESC
  )
`;
};

function formatTxnInsightsRow(row: TxnInsightsResponseRow): TxnInsightEvent {
  const startTime = moment.utc(row.start_time);
  const endTime = moment.utc(row.end_time);
  const insights = getInsightsFromProblemsAndCauses(
    row.problems,
    row.causes,
    InsightExecEnum.TRANSACTION,
  );
  return {
    sessionID: row.session_id,
    transactionExecutionID: row.txn_id,
    transactionFingerprintID: row.txn_fingerprint_id,
    implicitTxn: row.implicit_txn,
    query: row.query.split(" ; ").join("\n"),
    startTime,
    endTime,
    elapsedTimeMillis: endTime.diff(startTime, "milliseconds"),
    application: row.app_name,
    username: row.user_name,
    rowsRead: row.rows_read,
    rowsWritten: row.rows_written,
    priority: row.priority,
    retries: row.retries,
    lastRetryReason: row.last_retry_reason,
    contentionTime: moment.duration(row.contention ?? 0),
    insights,
    stmtExecutionIDs: row.stmt_execution_ids,
    cpuSQLNanos: row.cpu_sql_nanos,
  };
}

export type TxnInsightsRequest = {
  txnExecutionID?: string;
  txnFingerprintID?: string;
  start?: moment.Moment;
  end?: moment.Moment;
};

export function getTxnInsightsApi(
  req?: TxnInsightsRequest,
): Promise<TxnInsightEvent[]> {
  const filters: TxnQueryFilters = {
    start: req?.start,
    end: req?.end,
    execID: req?.txnExecutionID,
    fingerprintID: req?.txnFingerprintID,
  };
  const request = makeInsightsSqlRequest([createTxnInsightsQuery(filters)]);
  return executeInternalSql<TxnInsightsResponseRow>(request).then(result => {
    if (result.error) {
      throw new Error(
        `Error while retrieving insights information: ${sqlApiErrorMessage(
          result.error.message,
        )}`,
      );
    }

    if (sqlResultsAreEmpty(result)) {
      return [];
    }
    return result.execution.txn_results[0].rows.map(formatTxnInsightsRow);
  });
}

export type TxnInsightDetailsRequest = {
  txnExecutionID: string;
  excludeStmts?: boolean;
  excludeTxn?: boolean;
  excludeContention?: boolean;
  mergeResultWith?: TxnInsightDetails;
  start?: moment.Moment;
  end?: moment.Moment;
};

export type TxnInsightDetailsReqErrs = {
  txnDetailsErr: Error | null;
  contentionErr: Error | null;
  statementsErr: Error | null;
};

export type TxnInsightDetailsResponse = {
  txnExecutionID: string;
  result: TxnInsightDetails;
  errors: TxnInsightDetailsReqErrs;
};

export async function getTxnInsightDetailsApi(
  req: TxnInsightDetailsRequest,
): Promise<TxnInsightDetailsResponse> {
  // All queries in this request read from virtual tables, which is an
  // expensive operation. To reduce the number of RPC fanouts, we have the
  // caller specify which parts of the txn details we should return, since
  // some parts may be available in the cache or are unnecessary to fetch
  // (e.g. when there is no high contention to report).
  //
  // Note the way we construct the object below is important. We spread the
  // the existing object fields into a new object in order to ensure a new
  // reference is returned so that components will be notified that there
  // was a change. However, we want the internal objects (e.g. txnDetails)
  // should only change when they are re-fetched so that components don't update
  // unnecessarily.
  const txnInsightDetails: TxnInsightDetails = { ...req.mergeResultWith };
  const errors: TxnInsightDetailsReqErrs = {
    txnDetailsErr: null,
    contentionErr: null,
    statementsErr: null,
  };

  if (!req.excludeTxn) {
    const request = makeInsightsSqlRequest([
      createTxnInsightsQuery({
        execID: req?.txnExecutionID,
        start: req?.start,
        end: req?.end,
      }),
    ]);
    try {
      const result = await executeInternalSql<TxnInsightsResponseRow>(request);

      if (result.error) {
        throw new Error(
          `Error while retrieving insights information: ${sqlApiErrorMessage(
            result.error.message,
          )}`,
        );
      }

      const txnDetailsRes = result.execution.txn_results[0];
      if (txnDetailsRes.rows?.length) {
        const txnDetails = formatTxnInsightsRow(txnDetailsRes.rows[0]);
        txnInsightDetails.txnDetails = txnDetails;
      }
    } catch (e) {
      errors.txnDetailsErr = e;
    }
  }

  if (!req.excludeStmts) {
    try {
      const request = makeInsightsSqlRequest([
        stmtInsightsByTxnExecutionQuery(req.txnExecutionID),
      ]);

      const result = await executeInternalSql<StmtInsightsResponseRow>(request);

      if (result.error) {
        throw new Error(
          `Error while retrieving insights information: ${sqlApiErrorMessage(
            result.error.message,
          )}`,
        );
      }

      const stmts = result.execution.txn_results[0];
      if (stmts.rows?.length) {
        txnInsightDetails.statements = formatStmtInsights(stmts);
      }
    } catch (e) {
      errors.statementsErr = e;
    }
  }

  const highContention = txnInsightDetails.txnDetails?.insights?.some(
    insight => insight.name === InsightNameEnum.highContention,
  );

  try {
    if (!req.excludeContention && highContention) {
      const contentionInfo = await getTxnInsightsContentionDetailsApi(req);
      txnInsightDetails.blockingContentionDetails =
        contentionInfo?.blockingContentionDetails;
    }
  } catch (e) {
    errors.contentionErr = e;
  }

  return {
    txnExecutionID: req.txnExecutionID,
    result: txnInsightDetails,
    errors,
  };
}

// Transaction Insight Counts

// Note that insight counts show the number of distinct insight types for a given execution, not the number of
// individual insight events.
export type TransactionInsightCounts = ExecutionInsightCountEvent[];

const txnInsightCountsQuery = (filters?: TxnQueryFilters) => {
  const txnColumns = `
  encode(txn_fingerprint_id, 'hex') AS txn_fingerprint_id,
  problems,
  causes`;

  let whereClause = `
WHERE app_name NOT LIKE '${INTERNAL_APP_NAME_PREFIX}%'
AND txn_id != '00000000-0000-0000-0000-000000000000'`;

  if (filters?.start) {
    whereClause += ` AND start_time >= '${filters.start.toISOString()}'`;
  }

  if (filters?.end) {
    whereClause += ` AND end_time <= '${filters.end.toISOString()}'`;
  }

  return `
    SELECT DISTINCT ON (txn_fingerprint_id, problems, causes)
      ${txnColumns}
    FROM
      ${TXN_INSIGHTS_TABLE_NAME}
    ${whereClause}
    ORDER BY txn_fingerprint_id, problems, causes, end_time DESC
`;
};

type TransactionInsightCountResponseRow = {
  txn_fingerprint_id: string; // hex string
  problems: string[];
  causes: string[];
};

function getTransactionInsightCountResponse(
  response: SqlExecutionResponse<TransactionInsightCountResponseRow>,
): TransactionInsightCounts {
  if (!response.execution.txn_results[0].rows) {
    return [];
  }

  const txnInsightMap = new Map<string, Set<InsightNameEnum>>();
  response.execution.txn_results[0].rows.forEach(row => {
    const txnInsights = getInsightsFromProblemsAndCauses(
      row.problems,
      row.causes,
      InsightExecEnum.TRANSACTION,
    );
    if (!txnInsightMap.has(row.txn_fingerprint_id)) {
      const txnInsightTypes = new Set<InsightNameEnum>();
      txnInsights.forEach(insight => txnInsightTypes.add(insight.name));
      txnInsightMap.set(row.txn_fingerprint_id, txnInsightTypes);
    } else {
      txnInsights.forEach(insight => {
        const mapValues = txnInsightMap.get(row.txn_fingerprint_id);
        !mapValues.has(insight.name) && mapValues.add(insight.name);
      });
    }
  });

  const res: TransactionInsightCounts = Array.from(
    txnInsightMap,
    ([name, value]) => ({ fingerprintID: name, insightCount: value.size }),
  );

  return res;
}

export function getTransactionInsightCount(
  req: TxnInsightsRequest,
): Promise<TransactionInsightCounts> {
  const request = makeInsightsSqlRequest([txnInsightCountsQuery(req)]);
  return executeInternalSql<TransactionInsightCountResponseRow>(request).then(
    result => {
      return getTransactionInsightCountResponse(result);
    },
  );
}
