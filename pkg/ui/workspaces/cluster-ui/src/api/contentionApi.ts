// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";

import {
  ContentionDetails,
  ContentionTypeKey,
  InsightExecEnum,
  InsightNameEnum,
  TxnContentionInsightDetails,
} from "src/insights/types";

import { FixFingerprintHexValue, getLogger } from "../util";

import {
  executeInternalSql,
  formatApiResult,
  INTERNAL_SQL_API_APP,
  LARGE_RESULT_SIZE,
  LONG_TIMEOUT,
  sqlApiErrorMessage,
  SqlApiResponse,
  SqlExecutionRequest,
  SqlExecutionResponse,
  sqlResultsAreEmpty,
} from "./sqlApi";
import { TxnInsightDetailsRequest } from "./txnInsightDetailsApi";
import {
  FingerprintStmtsResponseColumns,
  TxnStmtFingerprintsResponseColumns,
  TxnWithStmtFingerprints,
} from "./txnInsightsApi";
import { makeInsightsSqlRequest } from "./txnInsightsUtils";

export type ContentionFilters = {
  waitingTxnID?: string;
  waitingStmtID?: string;
  start?: moment.Moment;
  end?: moment.Moment;
};

export type ContentionResponseColumns = {
  waiting_txn_id: string;
  waiting_txn_fingerprint_id: string;
  collection_ts: string;
  contention_duration: string;
  blocking_txn_id: string;
  blocking_txn_fingerprint_id: string;
  waiting_stmt_id: string;
  waiting_stmt_fingerprint_id: string;
  schema_name: string;
  database_name: string;
  table_name: string;
  index_name: string;
  key: string;
  contention_type: ContentionTypeKey;
};

export async function getContentionDetailsApi(
  filters?: ContentionFilters,
): Promise<SqlApiResponse<ContentionDetails[]>> {
  const request: SqlExecutionRequest = {
    statements: [
      {
        sql: contentionDetailsQuery(filters),
      },
    ],
    execute: true,
    max_result_size: LARGE_RESULT_SIZE,
    timeout: LONG_TIMEOUT,
  };

  const result = await executeInternalSql<ContentionResponseColumns>(request);

  if (sqlResultsAreEmpty(result)) {
    if (result?.error) {
      // We don't return an error if it failed to retrieve the contention information.
      getLogger().error(
        "Insights encounter an error while retrieving contention information.",
        { resultError: result.error },
      );
    }
    return formatApiResult<ContentionDetails[]>(
      [],
      null,
      "retrieving contention information",
    );
  }

  const contentionDetails: ContentionDetails[] = [];
  result.execution?.txn_results.forEach(x => {
    x.rows.forEach(row => {
      contentionDetails.push({
        blockingExecutionID: row.blocking_txn_id,
        blockingTxnFingerprintID: FixFingerprintHexValue(
          row.blocking_txn_fingerprint_id,
        ),
        blockingTxnQuery: null,
        waitingTxnID: row.waiting_txn_id,
        waitingTxnFingerprintID: row.waiting_txn_fingerprint_id,
        waitingStmtID: row.waiting_stmt_id,
        waitingStmtFingerprintID: row.waiting_stmt_fingerprint_id,
        collectionTimeStamp: moment(row.collection_ts).utc(),
        contendedKey: row.key,
        contentionTimeMs: moment
          .duration(row.contention_duration)
          .asMilliseconds(),
        databaseName: row.database_name,
        schemaName: row.schema_name,
        tableName: row.table_name,
        indexName:
          row.index_name && row.index_name !== ""
            ? row.index_name
            : "index not found",
        contentionType: row.contention_type,
      });
    });
  });

  return formatApiResult<ContentionDetails[]>(
    contentionDetails,
    result.error,
    "retrieving insights information",
  );
}

function isFiltered(filters: ContentionFilters): boolean {
  if (filters == null) {
    return false;
  }

  return (
    filters.waitingStmtID != null ||
    filters.waitingTxnID != null ||
    filters.end != null ||
    filters.start != null
  );
}

function getContentionWhereClause(filters?: ContentionFilters): string {
  if (!isFiltered(filters)) {
    return "";
  }
  const defaultWhereClause = " where ";
  let whereClause = defaultWhereClause;
  if (filters?.waitingStmtID) {
    whereClause =
      whereClause + ` waiting_stmt_id = '${filters.waitingStmtID}' `;
  }

  if (filters?.waitingTxnID) {
    if (whereClause !== defaultWhereClause) {
      whereClause += " and ";
    }
    whereClause = whereClause + ` waiting_txn_id = '${filters.waitingTxnID}' `;
  }

  if (filters?.start) {
    if (whereClause !== defaultWhereClause) {
      whereClause += " and ";
    }
    whereClause =
      whereClause + ` collection_ts >= '${filters.start.toISOString()}' `;
  }
  if (filters?.end) {
    if (whereClause !== defaultWhereClause) {
      whereClause += " and ";
    }

    whereClause =
      whereClause +
      ` (collection_ts + contention_duration) <= '${filters.end.toISOString()}' `;
  }
  return whereClause;
}

// txnContentionDetailsQuery selects information about a specific transaction contention event.
function contentionDetailsQuery(filters?: ContentionFilters) {
  const whereClause = getContentionWhereClause(filters);
  return `
    SELECT DISTINCT collection_ts,
                    blocking_txn_id,
                    encode(blocking_txn_fingerprint_id, 'hex') AS blocking_txn_fingerprint_id,
                    waiting_txn_id,
                    encode(waiting_txn_fingerprint_id, 'hex')  AS waiting_txn_fingerprint_id,
                    waiting_stmt_id,
                    encode(waiting_stmt_fingerprint_id, 'hex') AS waiting_stmt_fingerprint_id,
                    contention_duration,
                    contending_pretty_key AS key,
                    database_name,
                    schema_name,
                    table_name,
                    index_name,
                    contention_type
    FROM
      crdb_internal.transaction_contention_events
      ${whereClause}
  `;
}

type PartialTxnContentionDetails = Omit<
  TxnContentionInsightDetails,
  "application" | "queries"
>;

function formatTxnContentionDetailsResponse(
  response: ContentionDetails[],
): PartialTxnContentionDetails {
  if (!response || response.length === 9) {
    // No data.
    return;
  }

  const row = response[0];
  return {
    transactionExecutionID: row.waitingTxnID,
    transactionFingerprintID: FixFingerprintHexValue(
      row.waitingTxnFingerprintID,
    ),
    blockingContentionDetails: response,
    insightName: InsightNameEnum.HIGH_CONTENTION,
    execType: InsightExecEnum.TRANSACTION,
  };
}

function buildTxnContentionInsightDetails(
  partialTxnContentionDetails: PartialTxnContentionDetails,
  txnsWithStmtFingerprints: TxnWithStmtFingerprints[],
  stmtFingerprintToQuery: StmtFingerprintToQueryRecord,
): TxnContentionInsightDetails {
  if (!partialTxnContentionDetails && !txnsWithStmtFingerprints.length) {
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

    blockedRow.blockingTxnQuery = currBlockedFingerprintStmts.queryIDs.map(
      id =>
        stmtFingerprintToQuery.get(id) ??
        `Query unavailable for statement fingerprint ${id}`,
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

type StmtFingerprintToQueryRecord = Map<
  string, // Key = Stmt fingerprint ID
  string // Value = query string
>;

function createStmtFingerprintToQueryMap(
  response: SqlExecutionResponse<FingerprintStmtsResponseColumns>,
): StmtFingerprintToQueryRecord {
  const idToQuery: Map<string, string> = new Map();
  if (!response || sqlResultsAreEmpty(response)) {
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

// Query to select all statement queries for each requested statement
// fingerprint.
const fingerprintStmtsQuery = (stmtFingerprintIDs: string[]): string => `
SELECT
  DISTINCT ON (fingerprint_id) encode(fingerprint_id, 'hex') AS statement_fingerprint_id,
  (metadata ->> 'query') AS query
FROM crdb_internal.statement_statistics_persisted
WHERE encode(fingerprint_id, 'hex') =
      ANY ARRAY[ ${stmtFingerprintIDs.map(id => `'${id}'`).join(",")} ]`;

// txnStmtFingerprintsQuery selects all statement fingerprints for each
// requested transaction fingerprint.
const txnStmtFingerprintsQuery = (txnFingerprintIDs: string[]) => `
SELECT
  DISTINCT ON (fingerprint_id) encode(fingerprint_id, 'hex') AS transaction_fingerprint_id,
  app_name,
  ARRAY( SELECT jsonb_array_elements_text(metadata -> 'stmtFingerprintIDs' )) AS query_ids
FROM crdb_internal.transaction_statistics_persisted
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
    queryIDs: row.query_ids.map(id => FixFingerprintHexValue(id)),
    application: row.app_name,
  }));
}

export async function getTxnInsightsContentionDetailsApi(
  req: Pick<TxnInsightDetailsRequest, "txnExecutionID">,
): Promise<TxnContentionInsightDetails> {
  // Note that any errors encountered fetching these results are caught
  // earlier in the call stack.
  //
  // There are 3 api requests/queries in this process.
  // 1. Get contention insight for the requested transaction.
  // 2. Get the stmt fingerprints for ALL transactions involved in the contention.
  // 3. Get the query strings for ALL statements involved in the transaction.

  // Get contention results for requested transaction.

  const contentionResponse = await getContentionDetailsApi({
    waitingTxnID: req.txnExecutionID,
  });
  const contentionResults = contentionResponse.results;

  if (contentionResults.length === 0) {
    return null;
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
  let stmtQueriesResponse: SqlExecutionResponse<FingerprintStmtsResponseColumns> | null =
    null;

  if (stmtFingerprintIDs.size) {
    stmtQueriesResponse =
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
  }

  return buildTxnContentionInsightDetails(
    contentionDetails,
    txnsWithStmtFingerprints,
    createStmtFingerprintToQueryMap(stmtQueriesResponse),
  );
}
