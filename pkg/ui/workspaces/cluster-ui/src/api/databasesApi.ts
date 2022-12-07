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
import { withTimeout } from "./util";
import moment from "moment";

export type DatabasesColumns = {
  database_name: string;
  owner: string;
  primary_region: string;
  secondary_region: string;
  regions: string[];
  survival_goal: string;
};

export type DatabasesListResponse = { databases: string[] };

export const databasesRequest: SqlExecutionRequest = {
  statements: [
    {
      sql: `SHOW DATABASES`,
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
    // If request succeeded but query failed, throw error (caught by saga/cacheDataReducer).
    if (result.error) {
      throw result.error;
    }

    if (sqlResultsAreEmpty(result)) {
      return { databases: [] };
    }

    const dbNames: string[] = result.execution.txn_results[0].rows.map(
      row => row.database_name,
    );

    return { databases: dbNames };
  });
}
