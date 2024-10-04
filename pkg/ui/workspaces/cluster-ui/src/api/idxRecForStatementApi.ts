// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { SqlExecutionRequest, executeInternalSql } from "./sqlApi";

export type IndexRecommendationsResponse = {
  recommendations?: string[];
  error?: Error;
};

export type IndexRecommendationsRequest = {
  planGist: string;
  appName: string;
  query: string;
};

type IndexRecommendationsColumns = {
  index_recommendations: string[];
};

/**
 * getIdxRecommendationsFromExecution gets the latest index recommendation from the same
 * plan gist, app name and query combination.
 * @param req the request providing the planGist
 */
export function getIdxRecommendationsFromExecution(
  req: IndexRecommendationsRequest,
): Promise<IndexRecommendationsResponse> {
  const request: SqlExecutionRequest = {
    statements: [
      {
        sql: `SELECT index_recommendations 
                from 
                    crdb_internal.statement_statistics 
                where
                  statistics -> 'statistics' -> 'planGists' ->> 0 = '${req.planGist}'
                  AND app_name = '${req.appName}'
                  AND metadata ->> 'query' = '${req.query}'
                ORDER BY aggregated_ts DESC 
                LIMIT 1`,
      },
    ],
    execute: true,
    timeout: "30s",
  };

  return executeInternalSql<IndexRecommendationsColumns>(request).then(
    result => {
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

      const recommendations =
        result.execution.txn_results[0].rows.length > 0
          ? result.execution.txn_results[0].rows[0].index_recommendations
          : [];

      return { recommendations };
    },
  );
}
