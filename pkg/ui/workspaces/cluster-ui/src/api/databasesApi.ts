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
  SqlExecutionErrorMessage,
  SqlExecutionRequest,
  sqlResultsAreEmpty,
} from "./sqlApi";
import { withTimeout } from "./util";
import moment from "moment-timezone";

export type DatabasesColumns = {
  database_name: string;
};

export type DatabasesListResponse = {
  databases: string[];
  error: SqlExecutionErrorMessage;
};

export const databasesRequest: SqlExecutionRequest = {
  statements: [
    {
      sql: `select database_name
            from [show databases]`,
    },
  ],
  execute: true,
  max_result_size: LARGE_RESULT_SIZE,
};

// getDatabasesList fetches databases names from the database. Callers of
// getDatabasesList from cluster-ui will need to pass a timeout argument for
// promise timeout handling (callers from db-console already have promise
// timeout handling as part of the cacheDataReducer).
export function getDatabasesList(
  timeout?: moment.Duration,
): Promise<DatabasesListResponse> {
  return withTimeout(
    executeInternalSql<DatabasesColumns>(databasesRequest),
    timeout,
  ).then(result => {
    // If there is an error and there are no result throw error.
    const noTxnResultsExist = result?.execution?.txn_results?.length === 0;
    if (
      result.error &&
      (noTxnResultsExist || result.execution.txn_results[0].rows.length === 0)
    ) {
      throw result.error;
    }

    if (sqlResultsAreEmpty(result)) {
      return { databases: [], error: result.error };
    }

    const dbNames: string[] = result.execution.txn_results[0].rows.map(
      row => row.database_name,
    );

    return { databases: dbNames, error: result.error };
  });
}
