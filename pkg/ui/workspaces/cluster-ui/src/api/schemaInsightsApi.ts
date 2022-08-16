// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { executeSql, SqlExecutionRequest, SqlTxnResult } from "./sqlApi";
import {InsightRecommendation, InsightType, recommendDropUnusedIndex} from "../insights";
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
  toState: (response: SqlTxnResult<RowType>) => InsightRecommendation[];
};

function clusterIndexUsageStatsToEventState(
  txn_result: SqlTxnResult<ClusterIndexUsageStatistic>,
): InsightRecommendation[] {
  const results: Record<string, InsightRecommendation> = {};

  txn_result.rows.forEach(row => {
    const result = recommendDropUnusedIndex(row);
    if (result.recommend) {
      const key = row.table_id.toString() + row.index_id.toString();
      if (!results[key]) {
        results[key] = {
          type: "DROP_INDEX",
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

function createIndexRecommendationsToState(
  txn_result: SqlTxnResult<CreateIndexRecommendationsResponse>,
): InsightRecommendation[] {
  const results: InsightRecommendation[] = [];

  txn_result.rows.forEach(row => {
    row.index_recommendations.forEach(rec => {
      results.push({
        type: "CREATE_INDEX",
        database: row.db,
        execution: {
          statement: row.query,
          summary: row.querysummary,
          fingerprintID: HexStringToInt64String(row.fingerprint_id),
          implicit: row.implicittxn,
        },
        query: rec.split(" : ")[1],
      });
    });
  });
  return results;
}

const dropUnusedIndexQuery: SchemaInsightQuery<ClusterIndexUsageStatistic> = {
  name: "DROP_INDEX",
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
  toState: clusterIndexUsageStatsToEventState,
};

const createIndexRecommendationsQuery: SchemaInsightQuery<CreateIndexRecommendationsResponse> =
  {
    name: "CREATE_INDEX",
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
      FROM crdb_internal.statement_statistics) 
      WHERE rank=1 AND array_length(index_recommendations,1) > 0;`,
    toState: createIndexRecommendationsToState,
  };

const schemaInsightQueries: SchemaInsightQuery<SchemaInsightResponse>[] = [
  dropUnusedIndexQuery,
  createIndexRecommendationsQuery,
];

// getSchemaInsightEventState is currently hardcoded to use the High Wait Time insight type
// for transaction contention events
export function getSchemaInsightEventState(): Promise<InsightRecommendation[]> {
  const request: SqlExecutionRequest = {
    statements: schemaInsightQueries.map(insightQuery => ({
      sql: insightQuery.query,
    })),
    execute: true,
  };
  return executeSql<SchemaInsightResponse>(request).then(result => {
    const results: InsightRecommendation[] = [];
    if (result.execution.txn_results.length === 0) {
      // No data.
      return results;
    }

    // Iterate through each txn_result, call the corresponding "toState" method, push results to InsightRecommendation[]
    result.execution.txn_results.map(txn_result => {
      // Note: txn_result.statement values begin at 1, not 0.
      const insightQuery: SchemaInsightQuery<SchemaInsightResponse> =
        schemaInsightQueries[txn_result.statement - 1];
      if (txn_result.rows) {
        results.push(...insightQuery.toState(txn_result));
      }
    });
    return results;
  });
}
