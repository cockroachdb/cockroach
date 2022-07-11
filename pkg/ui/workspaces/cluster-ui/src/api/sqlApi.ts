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
  database_name?: string; // Defaults to defaultDb
  max_result_size?: number; // Default 10kib
};

export type SqlStatement = {
  sql: string;
  arguments?: unknown[];
};

export type SqlExecutionResponse<T> = {
  num_statements?: number;
  execution: SqlExecutionExecResult<T>;
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
  source: { file: string; line: number; function: "string" };
};

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
  return fetchDataJSON<SqlExecutionResponse<RowType>, SqlExecutionRequest>(
    SQL_API_PATH,
    req,
  );
}
