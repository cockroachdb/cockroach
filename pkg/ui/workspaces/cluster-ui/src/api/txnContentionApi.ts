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
  SqlExecutionRequest,
  sqlResultsAreEmpty,
} from "./sqlApi";

// ContentionEventResponseColumns represents the columns returned by the
// txnContentionQuery.
export type ContentionEventResponseColumns = {
  collection_ts: string;
  blocking_txn_id: string;
  blocking_txn_fingerprint_id: string;
  waiting_txn_id: string;
  waiting_txn_fingerprint_id: string;
  contention_duration: string;
  contending_key: string;
  query: string;
};

// ContentionEvent represents a single transaction contention event.
type ContentionEvent = {
  waitingTxnExecutionID: string;
  blockingTxnExecutionID: string;
  statements?: string[];
  waitingTxnFingerprintID?: string;
  collectedAt: string;
  contentionDuration: string;
};

export type ContentionEventsResponse = ContentionEvent[];

// Query to find transaction contention events and their corresponding SQL
// statement by joining the `crdb_internal.transaction_contention_events` and
// the `crdb_internal.statement_statistics` virtual tables on their shared
// transaction fingerprint ID column.
const txnContentionQuery = `
SELECT 
  collection_ts, 
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
  prettify_statement(metadata ->> 'query', 108, 1, 1) AS query 
FROM 
  crdb_internal.transaction_contention_events AS tce 
  INNER JOIN crdb_internal.statement_statistics AS ss ON tce.waiting_txn_fingerprint_id = ss.transaction_fingerprint_id;`;

export const transactionContentionRequest: SqlExecutionRequest = {
  statements: [{ sql: txnContentionQuery }],
  execute: true,
  max_result_size: LARGE_RESULT_SIZE,
};

// getTxnContentionEvents returns a list of transaction contention events from
// the `crdb_internal.transaction_contention_events` virtual table.
export async function getTxnContentionEvents(): Promise<ContentionEventsResponse> {
  return executeInternalSql<ContentionEventResponseColumns>(
    transactionContentionRequest,
  ).then(result => {
    if (result.error) {
      throw result.error;
    }

    if (sqlResultsAreEmpty(result)) {
      return [];
    }

    let events: ContentionEvent[];

    result.execution.txn_results[0].rows.forEach(row => {
      events.push({
        waitingTxnExecutionID: row.waiting_txn_id,
        blockingTxnExecutionID: row.blocking_txn_id,
        statements: [row.query],
        waitingTxnFingerprintID: row.blocking_txn_fingerprint_id,
        collectedAt: row.collection_ts,
        contentionDuration: row.contention_duration,
      });
    });

    return events;
  });
}
