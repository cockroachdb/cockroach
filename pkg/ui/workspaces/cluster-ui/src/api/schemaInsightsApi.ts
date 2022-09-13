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
  SqlExecutionRequest,
  SqlTxnResult,
  executeInternalSql,
} from "./sqlApi";
import {
  InsightRecommendation,
  InsightType,
  recommendDropUnusedIndex,
} from "../insights";
import { HexStringToInt64String } from "../util";

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
};

type CreateIndexRecommendationsResponse = {
  fingerprint_id: string;
  db: string;
  query: string;
  querysummary: string;
  implicittxn: boolean;
  index_recommendations: string[];
};

type SchemaInsightResponse =
  | ClusterIndexUsageStatistic
  | CreateIndexRecommendationsResponse;
type SchemaInsightQuery<RowType> = {
  name: InsightType;
  query: string;
  toSchemaInsight: (response: SqlTxnResult<RowType>) => InsightRecommendation[];
};

function clusterIndexUsageStatsToSchemaInsight(
  txn_result: SqlTxnResult<ClusterIndexUsageStatistic>,
): InsightRecommendation[] {
  const results: Record<string, InsightRecommendation> = {};

  txn_result.rows.forEach(row => {
    const result = recommendDropUnusedIndex(row);
    if (result.recommend) {
      const key = row.table_id.toString() + row.index_id.toString();
      if (!results[key]) {
        results[key] = {
          type: "DropIndex",
          database: row.database_name,
          query: `DROP INDEX ${row.table_name}@${row.index_name};`,
          indexDetails: {
            table: row.table_name,
            indexID: row.index_id,
            indexName: row.index_name,
            lastUsed: result.reason,
          },
        };
      }
    }
  });

  return Object.values(results);
}

function createIndexRecommendationsToSchemaInsight(
  txn_result: SqlTxnResult<CreateIndexRecommendationsResponse>,
): InsightRecommendation[] {
  const results: InsightRecommendation[] = [];

  txn_result.rows.forEach(row => {
    row.index_recommendations.forEach(rec => {
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

const dropUnusedIndexQuery: SchemaInsightQuery<ClusterIndexUsageStatistic> = {
  name: "DropIndex",
  query: `SELECT
            us.table_id,
            us.index_id,
            us.last_read,
            ti.created_at,
            ti.index_name,
            t.name as table_name,
            t.parent_id as database_id,
            t.database_name,
            (SELECT value FROM crdb_internal.cluster_settings WHERE variable = 'sql.index_recommendation.drop_unused_duration') AS unused_threshold
          FROM "".crdb_internal.index_usage_statistics AS us
                 JOIN "".crdb_internal.table_indexes as ti ON us.index_id = ti.index_id AND us.table_id = ti.descriptor_id
                 JOIN "".crdb_internal.tables as t ON t.table_id = ti.descriptor_id and t.name = ti.descriptor_name
          WHERE t.database_name != 'system' AND ti.index_type != 'primary';`,
  toSchemaInsight: clusterIndexUsageStatsToSchemaInsight,
};

const createIndexRecommendationsQuery: SchemaInsightQuery<CreateIndexRecommendationsResponse> =
  {
    name: "CreateIndex",
    query: `SELECT
       encode(fingerprint_id, 'hex') AS fingerprint_id,  
       metadata ->> 'db' AS db, 
       metadata ->> 'query' AS query, 
       metadata ->> 'querySummary' as querySummary, 
       metadata ->> 'implicitTxn' AS implicitTxn, 
       index_recommendations 
    FROM (
      SELECT 
        fingerprint_id, 
        statistics -> 'statistics' ->> 'lastExecAt' as lastExecAt, 
        metadata, 
        index_recommendations, 
        row_number() over(
          PARTITION BY 
            fingerprint_id 
          ORDER BY statistics -> 'statistics' ->> 'lastExecAt' DESC
        ) AS rank 
      FROM crdb_internal.statement_statistics WHERE aggregated_ts >= now() - INTERVAL '1 week')
      WHERE rank=1 AND array_length(index_recommendations,1) > 0;`,
    toSchemaInsight: createIndexRecommendationsToSchemaInsight,
  };

const schemaInsightQueries: SchemaInsightQuery<SchemaInsightResponse>[] = [
  dropUnusedIndexQuery,
  createIndexRecommendationsQuery,
];

// getSchemaInsights makes requests over the SQL API and transforms the corresponding
// SQL responses into schema insights.
export function getSchemaInsights(): Promise<InsightRecommendation[]> {
  const request: SqlExecutionRequest = {
    statements: schemaInsightQueries.map(insightQuery => ({
      sql: insightQuery.query,
    })),
    execute: true,
  };
  return executeInternalSql<SchemaInsightResponse>(request).then(result => {
    const results: InsightRecommendation[] = [];
    if (result.execution.txn_results.length === 0) {
      // No data.
      return results;
    }

    result.execution.txn_results.map(txn_result => {
      // Note: txn_result.statement values begin at 1, not 0.
      const insightQuery: SchemaInsightQuery<SchemaInsightResponse> =
        schemaInsightQueries[txn_result.statement - 1];
      if (txn_result.rows) {
        results.push(...insightQuery.toSchemaInsight(txn_result));
      }
    });
    return results;
  });
}
