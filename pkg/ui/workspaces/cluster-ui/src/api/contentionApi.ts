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
  LARGE_RESULT_SIZE,
  LONG_TIMEOUT,
  SqlApiResponse,
  SqlExecutionRequest,
  sqlResultsAreEmpty,
  formatApiResult,
} from "./sqlApi";
import { ContentionDetails } from "src/insights";
import moment from "moment-timezone";

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
    if (result.error) {
      // We don't return an error if it failed to retrieve the contention information.
      console.error(
        `Insights encounter an error while retrieving contention information: ${result.error}`,
      );
    }
    return formatApiResult<ContentionDetails[]>(
      [],
      null,
      "retrieving contention information",
    );
  }

  const contentionDetails: ContentionDetails[] = [];
  result.execution.txn_results.forEach(x => {
    x.rows.forEach(row => {
      contentionDetails.push({
        blockingExecutionID: row.blocking_txn_id,
        blockingTxnFingerprintID: row.blocking_txn_fingerprint_id,
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
    if (whereClause != defaultWhereClause) {
      whereClause += " and ";
    }
    whereClause = whereClause + ` waiting_txn_id >= '${filters.waitingTxnID}' `;
  }

  if (filters?.start) {
    if (whereClause != defaultWhereClause) {
      whereClause += " and ";
    }
    whereClause =
      whereClause + ` collection_ts >= '${filters.start.toISOString()}' `;
  }
  if (filters?.end) {
    if (whereClause != defaultWhereClause) {
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
  index_name
    FROM
      crdb_internal.transaction_contention_events AS tce
      ${whereClause}
  `;
}
