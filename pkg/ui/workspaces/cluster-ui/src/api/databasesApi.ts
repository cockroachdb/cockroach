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
  SqlExecutionRequest,
  sqlResultsAreEmpty,
} from "./sqlApi";
import { PROMISE_TIMEOUT, withTimeout } from "./util";
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

export const DatabasesRequest: SqlExecutionRequest = {
  statements: [
    {
      sql: `SHOW DATABASES`,
    },
  ],
  execute: true,
};

export function getDatabasesList(timeout: moment.Duration = PROMISE_TIMEOUT): Promise<DatabasesListResponse> {
  return withTimeout(executeInternalSql<DatabasesColumns>(DatabasesRequest), timeout).then(result  => {
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
