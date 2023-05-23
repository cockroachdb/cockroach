// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { fetchDataJSON } from "./fetchData";

export type SqlExecutionRequest = {
  statements: SqlStatement[];
  execute?: boolean;
  timeout?: string; // Default 5s
  application_name?: string; // Defaults to '$ api-v2-sql'
  database?: string; // Defaults to system
  max_result_size?: number; // Default 10kib
};

export type SqlStatement = {
  sql: string;
  arguments?: unknown[];
};

export type SqlExecutionResponse<T> = {
  num_statements?: number;
  execution?: SqlExecutionExecResult<T>;
  error?: SqlExecutionErrorMessage;
  request?: SqlExecutionRequest;
};

export interface SqlExecutionExecResult<T> {
  retries: number;
  txn_results: SqlTxnResult<T>[];
}

export type SqlTxnResult<RowType> = {
  statement: number; // Statement index from input array
  tag: string; // Short stmt tag
  start: string; // Start timestamp, encoded as RFC3339
  end: string; // End timestamp, encoded as RFC3339
  rows_affected: number;
  columns?: SqlResultColumn[];
  rows?: RowType[];
  error?: Error;
};

export type SqlResultColumn = {
  name: string;
  type: string;
  oid: number;
};

export type SqlExecutionErrorMessage = {
  message: string;
  code: string;
  severity: string;
  source: { file: string; line: number; function: string };
};

export type SqlApiResponse<ResultType> = {
  maxSizeReached: boolean;
  results: ResultType;
};

export type SqlApiQueryResponse<Result> = Result & { error?: Error };

export const SQL_API_PATH = "/api/v2/sql/";

/**
 * executeSql executes the provided SQL statements in a single transaction
 * over HTTP.
 *
 * @param req execution request details
 */
export function executeSql<RowType>(
  req: SqlExecutionRequest,
): Promise<SqlExecutionResponse<RowType>> {
  // TODO(maryliag) remove this part of code when cloud is updated with
  // a new CRDB release.
  if (!req.database) {
    req.database = FALLBACK_DB;
  }
  return fetchDataJSON<SqlExecutionResponse<RowType>, SqlExecutionRequest>(
    SQL_API_PATH,
    req,
  );
}

export const INTERNAL_SQL_API_APP = "$ internal-console";
export const LONG_TIMEOUT = "300s";
export const LARGE_RESULT_SIZE = 50000; // 50 kib
export const FALLBACK_DB = "system";

/**
 * executeInternalSql executes the provided SQL statements with
 * the app name set to the internal sql api app name above.
 * Note that technically all SQL executed over this API are
 * executed as internal, but we make this distinction using the
 * function name for when we want to execute user queries in the
 * future, where such queries should not have an internal app name.
 *
 * @param req execution request details
 */
export function executeInternalSql<RowType>(
  req: SqlExecutionRequest,
): Promise<SqlExecutionResponse<RowType>> {
  if (!req.application_name) {
    req.application_name = INTERNAL_SQL_API_APP;
  } else {
    req.application_name = `$ internal-${req.application_name}`;
  }

  return executeSql(req);
}

/**
 * sqlResultsAreEmpty returns true if the provided result
 * does not contain any rows.
 * @param result the sql execution result returned by the server
 * @returns
 */
export function sqlResultsAreEmpty(
  result: SqlExecutionResponse<unknown>,
): boolean {
  return (
    !result.execution?.txn_results?.length ||
    result.execution.txn_results.every(txn => txnResultIsEmpty(txn))
  );
}

// Error messages relating to upgrades in progress.
// This is a temporary solution until we can use different queries for
// different versions. For now we just try to give more info as to why
// this page is unavailable for insights.
const UPGRADE_RELATED_ERRORS = [
  /relation "(.*)" does not exist/i,
  /column "(.*)" does not exist/i,
];

export function isUpgradeError(message: string): boolean {
  return UPGRADE_RELATED_ERRORS.some(err => message.search(err) !== -1);
}

/**
 * errorMessage cleans the error message returned by the sqlApi,
 * removing information not useful for the user.
 * e.g. the error message
 * "$executing stmt 1: run-query-via-api: only users with either MODIFYCLUSTERSETTING
 * or VIEWCLUSTERSETTING privileges are allowed to show cluster settings"
 * became
 * "only users with either MODIFYCLUSTERSETTING or VIEWCLUSTERSETTING privileges are allowed to show cluster settings"
 * and the error message
 * "executing stmt 1: max result size exceeded"
 * became
 * "max result size exceeded"
 * @param message
 */
export function sqlApiErrorMessage(message: string): string {
  if (isUpgradeError(message)) {
    return "This page may not be available during an upgrade.";
  }

  message = message.replace("run-query-via-api: ", "");
  if (message.includes(":")) {
    return message.split(":")[1];
  }

  return message;
}

export function createSqlExecutionRequest(
  dbName: string,
  statements: SqlStatement[],
): SqlExecutionRequest {
  return {
    execute: true,
    statements: statements,
    database: dbName,
    max_result_size: LARGE_RESULT_SIZE,
    timeout: LONG_TIMEOUT,
  };
}

export function isMaxSizeError(message: string): boolean {
  return !!message?.includes("max result size exceeded");
}

export function formatApiResult<ResultType>(
  results: ResultType,
  error: SqlExecutionErrorMessage,
  errorMessageContext: string,
): SqlApiResponse<ResultType> {
  const maxSizeError = isMaxSizeError(error?.message);

  if (error && !maxSizeError) {
    throw new Error(
      `Error while ${errorMessageContext}: ${sqlApiErrorMessage(
        error?.message,
      )}`,
    );
  }

  return {
    maxSizeReached: maxSizeError,
    results: results,
  };
}

export function txnResultIsEmpty(txn_result: SqlTxnResult<unknown>): boolean {
  return !txn_result.rows || txn_result.rows?.length === 0;
}
