// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { SqlExecutionRequest, executeInternalSql } from "./sqlApi";

export type DecodePlanGistResponse = {
  explainPlan?: string;
  error?: Error;
};

export type DecodePlanGistRequest = {
  planGist: string;
};

type DecodePlanGistColumns = {
  plan_row: string;
};

/**
 * getExplainPlanFromGist decodes the provided planGist into the logical
 * plan string.
 * @param req the request providing the planGist
 */
export function getExplainPlanFromGist(
  req: DecodePlanGistRequest,
): Promise<DecodePlanGistResponse> {
  const request: SqlExecutionRequest = {
    statements: [
      {
        sql: `SELECT crdb_internal.decode_plan_gist('${req.planGist}') as plan_row`,
      },
    ],
    execute: true,
    timeout: "30s",
  };

  return executeInternalSql<DecodePlanGistColumns>(request).then(result => {
    if (
      result.execution.txn_results.length === 0 ||
      !result.execution.txn_results[0].rows
    ) {
      return {
        error: result.execution.txn_results
          ? result.execution.txn_results[0].error
          : null,
      };
    }

    if (result.execution.txn_results[0].error) {
      return {
        error: result.execution.txn_results[0].error,
      };
    }

    const explainPlan =
      `Plan Gist: ${req.planGist} \n\n` +
      result.execution.txn_results[0].rows.map(col => col.plan_row).join("\n");

    return { explainPlan };
  });
}
