// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";

import { useSwrWithClusterId } from "../util";

import {
  executeInternalSql,
  LARGE_RESULT_SIZE,
  SqlExecutionErrorMessage,
  SqlExecutionRequest,
  sqlResultsAreEmpty,
} from "./sqlApi";
import { withTimeout } from "./util";

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
export async function getDatabasesList(
  timeout?: moment.Duration,
): Promise<DatabasesListResponse> {
  const resp = await withTimeout(
    executeInternalSql<DatabasesColumns>(databasesRequest),
    timeout,
  );
  // Encountered a response level error, or empty response.
  if (resp.error || sqlResultsAreEmpty(resp)) {
    return { databases: [], error: resp.error };
  }

  // Get database names.
  const dbNames: string[] = resp.execution.txn_results[0].rows.map(
    row => row.database_name,
  );

  // Note that we do not surface the txn_result error in the returned payload.
  // This request only contains a single txn_result, any error encountered by the txn_result
  // will be surfaced at the response level by the sql api.
  return { databases: dbNames, error: resp.error };
}

const DATABASES_LIST_SWR_KEY = "databasesList";

export function useDatabasesList() {
  const { data, error, isLoading } = useSwrWithClusterId<DatabasesListResponse>(
    { name: DATABASES_LIST_SWR_KEY },
    () => getDatabasesList(moment.duration(10, "m")),
    {
      revalidateOnFocus: false,
      revalidateOnReconnect: false,
    },
  );

  const databases = (data?.databases ?? [])
    .filter((dbName: string) => dbName !== null && dbName.length > 0)
    .sort();

  return { databases, error, isLoading };
}
