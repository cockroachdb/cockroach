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

// TransactionContentionResponseColumns represents the columns returned by the 
// txnContentionQuery.
export type TransactionContentionResponseColumns = {
    collection_ts: string;
    blocking_txn_id: string;
    blocking_txn_fingerprint_id: string;
    waiting_txn_id: string;
    waiting_txn_fingerprint_id: string;
    contention_duration: string;
    contending_key: string;
    metadata: {
        query: string;
    };
};

// Event represents a single transaction contention event.
type Event = {
    waiting_txn_id: string,
    blocking_txn_id: string,
    statement?: string,
    fingerprint?: string,
    timestamp: string,
    duration: string,
};

export type TransactionContentionEventsResponse = {
    events: Event[],
};

// Query to find transaction contention events and their corresponding SQL
// statement by joining the `crdb_internal.transaction_contention_events` and
// the `crdb_internal.statement_statistics` virtual tables on their shared
// transaction fingerprint ID column.
const txnContentionQuery = `
SELECT collection_ts, blocking_txn_id, blocking_txn_fingerprint_id, waiting_txn_id, waiting_txn_fingerprint_id, contention_duration, contending_key, metadata
FROM crdb_internal.transaction_contention_events
INNER JOIN crdb_internal.statement_statistics 
ON crdb_internal.transaction_contention_events.blocking_txn_fingerprint_id=crdb_internal.statement_statistics.transaction_fingerprint_id`;

export const transactionContentionRequest: SqlExecutionRequest = {
    statements: [
        { sql: txnContentionQuery },
    ],
    execute: true,
    max_result_size: LARGE_RESULT_SIZE,
};

// getTxnContentionEvents returns a list of transaction contention events from 
// the `crdb_internal.transaction_contention_events` virtual table.
export async function getTxnContentionEvents(): Promise<TransactionContentionEventsResponse> {
    return executeInternalSql<TransactionContentionResponseColumns>(
        transactionContentionRequest
    ).then(result => {
        if (result.error) {
            throw result.error;
        }

        if (sqlResultsAreEmpty(result)) {
            return {
                events: [],
            };
        }

        let events: Event[];

        result.execution.txn_results[0].rows.forEach(row => {
            events.push({
                waiting_txn_id: row.waiting_txn_id,
                blocking_txn_id: row.blocking_txn_id,
                statement: row.metadata.query,
                fingerprint: row.blocking_txn_fingerprint_id,
                timestamp: row.collection_ts,
                duration: row.contention_duration,
            });
        });

        return {
            events: events,
        };
    });
}