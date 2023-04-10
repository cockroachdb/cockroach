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
  SqlApiResponse,
  executeInternalSql,
  formatApiResult,
  INTERNAL_SQL_API_APP,
  LARGE_RESULT_SIZE,
  LONG_TIMEOUT,
  sqlApiErrorMessage,
  SqlExecutionRequest,
  SqlExecutionResponse,
  sqlResultsAreEmpty,
  isMaxSizeError,
} from "./sqlApi";
import {
  ContentionDetails,
  getInsightsFromProblemsAndCauses,
  InsightExecEnum,
  InsightNameEnum,
  TransactionStatus,
  TxnContentionInsightDetails,
  TxnInsightDetails,
  TxnInsightEvent,
} from "src/insights";
import moment from "moment-timezone";
import { FixFingerprintHexValue } from "../util";
import {
  formatStmtInsights,
  stmtInsightsByTxnExecutionQuery,
  StmtInsightsResponseRow,
} from "./stmtInsightsApi";
import { INTERNAL_APP_NAME_PREFIX } from "src/util/constants";
import { getContentionDetailsApi } from "./contentionApi";

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
  (metadata ->> 'query') AS query
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
    insightName: InsightNameEnum.highContention,
    execType: InsightExecEnum.TRANSACTION,
  };
}

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

  const contentionResponse = await getContentionDetailsApi({
    waitingTxnID: req.txnExecutionID,
    waitingStmtID: null,
    start: null,
    end: null,
  });
  const contentionResults = contentionResponse.results;

  if (contentionResults.length === 0) {
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

    blockedRow.blockingTxnQuery = currBlockedFingerprintStmts.queryIDs.map(
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
  last_error_code: string;
  status: TransactionStatus;
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
cpu_sql_nanos,
last_error_code,
status`;

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
    query: row.query?.split(" ; ").join("\n") || "",
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
    errorCode: row.last_error_code,
    status: row.status,
  };
}

export type TxnInsightsRequest = {
  txnExecutionID?: string;
  txnFingerprintID?: string;
  start?: moment.Moment;
  end?: moment.Moment;
};

export async function getTxnInsightsApi(
  req?: TxnInsightsRequest,
): Promise<SqlApiResponse<TxnInsightEvent[]>> {
  const filters: TxnQueryFilters = {
    start: req?.start,
    end: req?.end,
    execID: req?.txnExecutionID,
    fingerprintID: req?.txnFingerprintID,
  };
  const request = makeInsightsSqlRequest([createTxnInsightsQuery(filters)]);
  const result = await executeInternalSql<TxnInsightsResponseRow>(request);

  if (sqlResultsAreEmpty(result)) {
    return formatApiResult<TxnInsightEvent[]>(
      [],
      result.error,
      "retrieving insights information",
    );
  }

  return formatApiResult<TxnInsightEvent[]>(
    result.execution.txn_results[0].rows.map(formatTxnInsightsRow),
    result.error,
    "retrieving insights information",
  );
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
): Promise<SqlApiResponse<TxnInsightDetailsResponse>> {
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

  let maxSizeReached = false;
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
      maxSizeReached = isMaxSizeError(result.error?.message);

      if (result.error && !maxSizeReached) {
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
      const maxSizeStmtReached = isMaxSizeError(result.error?.message);

      if (result.error && !maxSizeStmtReached) {
        throw new Error(
          `Error while retrieving insights information: ${sqlApiErrorMessage(
            result.error.message,
          )}`,
        );
      }
      maxSizeReached = maxSizeReached || maxSizeStmtReached;

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
    maxSizeReached: maxSizeReached,
    results: {
      txnExecutionID: req.txnExecutionID,
      result: txnInsightDetails,
      errors,
    },
  };
}
