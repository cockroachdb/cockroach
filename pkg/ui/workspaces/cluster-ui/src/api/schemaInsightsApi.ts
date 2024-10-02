// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  InsightRecommendation,
  InsightType,
  recommendDropUnusedIndex,
} from "../insights";
import { HexStringToInt64String, indexUnusedDuration } from "../util";

import { QuoteIdentifier } from "./safesql";
import {
  SqlExecutionRequest,
  SqlTxnResult,
  executeInternalSql,
  LONG_TIMEOUT,
  sqlResultsAreEmpty,
  LARGE_RESULT_SIZE,
  SqlApiResponse,
  formatApiResult,
} from "./sqlApi";

// Export for db-console import from clusterUiApi.
export type { InsightRecommendation } from "../insights";

export type ClusterIndexUsageStatistic = {
  table_id: number;
  index_id: number;
  last_read?: string;
  created_at?: string;
  index_name: string;
  table_name: string;
  database_id: number;
  database_name: string;
  unused_threshold: string;
  schema_name: string;
};

type CreateIndexRecommendationsResponse = {
  fingerprint_id: string;
  db: string;
  query: string;
  querysummary: string;
  implicittxn: boolean;
  index_recommendations: string[];
};

export type SchemaInsightReqParams = {
  csIndexUnusedDuration: string;
};

type SchemaInsightResponse =
  | ClusterIndexUsageStatistic
  | CreateIndexRecommendationsResponse;
type SchemaInsightQuery<RowType> = {
  name: InsightType;
  query: string | ((csIndexUnusedDuration: string) => string);
  toSchemaInsight: (response: SqlTxnResult<RowType>) => InsightRecommendation[];
};

function clusterIndexUsageStatsToSchemaInsight(
  txnResult: SqlTxnResult<ClusterIndexUsageStatistic>,
): InsightRecommendation[] {
  const results: Record<string, InsightRecommendation> = {};

  txnResult.rows.forEach(row => {
    const result = recommendDropUnusedIndex(row);
    if (result.recommend) {
      const key = row.table_id.toString() + row.index_id.toString();
      if (!results[key]) {
        results[key] = {
          type: "DropIndex",
          database: row.database_name,
          query: `DROP INDEX ${QuoteIdentifier(
            row.schema_name,
          )}.${QuoteIdentifier(row.table_name)}@${QuoteIdentifier(
            row.index_name,
          )};`,
          indexDetails: {
            table: row.table_name,
            indexID: row.index_id,
            indexName: row.index_name,
            lastUsed: result.reason,
            schema: row.schema_name,
          },
        };
      }
    }
  });

  return Object.values(results);
}

function createIndexRecommendationsToSchemaInsight(
  txnResult: SqlTxnResult<CreateIndexRecommendationsResponse>,
): InsightRecommendation[] {
  const results: InsightRecommendation[] = [];

  txnResult.rows.forEach(row => {
    row.index_recommendations.forEach(rec => {
      if (!rec.includes(" : ")) {
        return;
      }
      const recSplit = rec.split(" : ");
      const recType = recSplit[0];
      const recQuery = recSplit[1];
      let idxType: InsightType;
      switch (recType) {
        case "creation":
          idxType = "CreateIndex";
          break;
        case "replacement":
          idxType = "ReplaceIndex";
          break;
        case "drop":
          idxType = "DropIndex";
          break;
        case "alteration":
          idxType = "AlterIndex";
          break;
      }

      results.push({
        type: idxType,
        database: row.db,
        execution: {
          statement: row.query,
          summary: row.querysummary,
          fingerprintID: HexStringToInt64String(row.fingerprint_id),
          implicit: row.implicittxn,
        },
        query: recQuery,
      });
    });
  });
  return results;
}

// This query have an ORDER BY for the cases where we reach the limit of the sql-api
// and want to return the most used ones as a priority.
const dropUnusedIndexQuery: SchemaInsightQuery<ClusterIndexUsageStatistic> = {
  name: "DropIndex",
  query: (csIndexUnusedDuration: string) => {
    csIndexUnusedDuration = csIndexUnusedDuration ?? indexUnusedDuration;
    return `SELECT * FROM (SELECT us.table_id,
                          us.index_id,
                          us.last_read,
                          us.total_reads,
                          ti.created_at,
                          ti.index_name,
                          t.name      as table_name,
                          t.parent_id as database_id,
                          t.database_name,
                          t.schema_name,
                          '${csIndexUnusedDuration}' as unused_threshold,
                          '${csIndexUnusedDuration}'::interval as interval_threshold, 
                          now() - COALESCE(us.last_read AT TIME ZONE 'UTC', COALESCE(ti.created_at, '0001-01-01')) as unused_interval
                   FROM "".crdb_internal.index_usage_statistics AS us
                            JOIN "".crdb_internal.table_indexes as ti
                                 ON us.index_id = ti.index_id AND us.table_id = ti.descriptor_id
                            JOIN "".crdb_internal.tables as t
                                 ON t.table_id = ti.descriptor_id and t.name = ti.descriptor_name
                   WHERE t.database_name != 'system' AND ti.is_unique IS false)
          WHERE unused_interval > interval_threshold
          ORDER BY total_reads DESC;`;
  },
  toSchemaInsight: clusterIndexUsageStatsToSchemaInsight,
};

const createIndexRecommendationsQuery: SchemaInsightQuery<CreateIndexRecommendationsResponse> =
  {
    name: "CreateIndex",
    query: `
SELECT
  encode(fingerprint_id, 'hex') AS fingerprint_id, 
  metadata ->> 'db' AS db, 
  metadata ->> 'query' AS query, 
  metadata ->> 'querySummary' as querySummary, 
  metadata ->> 'implicitTxn' AS implicitTxn, 
  index_recommendations 
FROM 
  (
    SELECT 
      fingerprint_id, 
      statistics -> 'statistics' ->> 'lastExecAt' as lastExecAt, 
      metadata, 
      index_recommendations, 
      row_number() over(
        PARTITION BY fingerprint_id 
        ORDER BY 
          statistics -> 'statistics' ->> 'lastExecAt' DESC
      ) AS rank 
    FROM 
      crdb_internal.statement_statistics_persisted 
    WHERE 
      aggregated_ts >= now() - INTERVAL '1 week'
  ) 
WHERE 
  rank = 1 AND array_length(index_recommendations, 1) > 0;
`,
    toSchemaInsight: createIndexRecommendationsToSchemaInsight,
  };

const schemaInsightQueries: Array<
  | SchemaInsightQuery<ClusterIndexUsageStatistic>
  | SchemaInsightQuery<CreateIndexRecommendationsResponse>
> = [dropUnusedIndexQuery, createIndexRecommendationsQuery];

function getQuery(
  csIndexUnusedDuration: string,
  query: string | ((csIndexUnusedDuration: string) => string),
): string {
  if (typeof query == "string") {
    return query;
  }
  return query(csIndexUnusedDuration);
}

// getSchemaInsights makes requests over the SQL API and transforms the corresponding
// SQL responses into schema insights.
export async function getSchemaInsights(
  params: SchemaInsightReqParams,
): Promise<SqlApiResponse<InsightRecommendation[]>> {
  const request: SqlExecutionRequest = {
    statements: schemaInsightQueries.map(insightQuery => ({
      sql: getQuery(params.csIndexUnusedDuration, insightQuery.query),
    })),
    execute: true,
    max_result_size: LARGE_RESULT_SIZE,
    timeout: LONG_TIMEOUT,
  };
  const result = await executeInternalSql<SchemaInsightResponse>(request);

  const results: InsightRecommendation[] = [];
  if (sqlResultsAreEmpty(result)) {
    return formatApiResult<InsightRecommendation[]>(
      [],
      result.error,
      "retrieving insights information",
    );
  }
  result.execution.txn_results.map(txnResult => {
    // Note: txn_result.statement values begin at 1, not 0.
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    const insightQuery: SchemaInsightQuery<SchemaInsightResponse> =
      schemaInsightQueries[txnResult.statement - 1];
    if (txnResult.rows) {
      results.push(...insightQuery.toSchemaInsight(txnResult));
    }
  });
  return formatApiResult<InsightRecommendation[]>(
    results,
    result.error,
    "retrieving insights information",
  );
}
