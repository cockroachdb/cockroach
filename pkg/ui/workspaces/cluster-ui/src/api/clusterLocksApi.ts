// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";

import {
  executeInternalSql,
  LONG_TIMEOUT,
  SqlExecutionRequest,
  sqlResultsAreEmpty,
  LARGE_RESULT_SIZE,
  SqlApiResponse,
  formatApiResult,
} from "./sqlApi";

export type ClusterLockState = {
  databaseName?: string;
  schemaName?: string;
  tableName?: string;
  indexName?: string;
  lockHolderTxnID?: string; // Excecution ID of txn holding this lock.
  holdTime: moment.Duration;
  waiters?: LockWaiter[]; // List of waiting transaction execution IDs.
};

export type LockWaiter = {
  id: string; // Txn execution ID.
  waitTime: moment.Duration;
};

export type ClusterLocksResponse = ClusterLockState[];

type ClusterLockColumns = {
  lock_key_pretty: string;
  database_name: string;
  schema_name: string;
  table_name: string;
  index_name: string;
  txn_id: string;
  duration: string;
  granted: boolean;
};

/**
 * getClusterLocksState returns information from crdb_internal.cluster_locks
 * regarding the state of range locks in the cluster.
 */
export function getClusterLocksState(): Promise<
  SqlApiResponse<ClusterLocksResponse>
> {
  const request: SqlExecutionRequest = {
    statements: [
      {
        sql: `
SELECT
  lock_key_pretty,
  database_name,
  schema_name,
  table_name,
  index_name,
  txn_id,
  duration,
  granted
FROM
  crdb_internal.cluster_locks
WHERE
  contended = true
`,
      },
    ],
    execute: true,
    timeout: LONG_TIMEOUT,
    max_result_size: LARGE_RESULT_SIZE,
  };

  return executeInternalSql<ClusterLockColumns>(request).then(result => {
    if (sqlResultsAreEmpty(result)) {
      return formatApiResult<ClusterLockState[]>(
        [],
        result.error,
        "retrieving cluster locks information",
      );
    }

    const locks: Record<string, ClusterLockState> = {};

    // If all the lock keys are blank, then the user has VIEWACTIVITYREDACTED
    // role. We won't be able to group the resulting rows by lock key to get
    // correlated transactions, but we can still surface wait time information.
    // To do this, we treat each entry as a unique lock entry with a single
    // txn.
    const noLockKeys = result.execution.txn_results[0].rows.every(
      row => !row.lock_key_pretty,
    );

    let counter = 0;
    result.execution.txn_results[0].rows.forEach(row => {
      const key = noLockKeys ? `entry_${counter++}` : row.lock_key_pretty;

      if (!locks[key]) {
        locks[key] = {
          databaseName: row.database_name,
          schemaName: row.schema_name,
          tableName: row.table_name,
          indexName: row.index_name,
          waiters: [],
          holdTime: moment.duration(),
        };
      }

      const duration = moment.duration(row.duration);
      if (row.granted) {
        locks[key].lockHolderTxnID = row.txn_id;
        locks[key].holdTime = duration;
      } else {
        locks[key].waiters.push({
          id: row.txn_id,
          waitTime: duration,
        });
      }
    });

    return formatApiResult<ClusterLockState[]>(
      Object.values(locks),
      result.error,
      "retrieving luster locks information",
    );
  });
}
