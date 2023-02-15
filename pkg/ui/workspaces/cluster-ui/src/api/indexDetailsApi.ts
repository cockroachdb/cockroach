// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import {
  convertStatementRawFormatToAggregatedStatistics,
  executeInternalSql,
  fetchData,
  LARGE_RESULT_SIZE,
  sqlApiErrorMessage,
  SqlExecutionRequest,
  sqlResultsAreEmpty,
  StatementRawFormat,
} from "src/api";
import moment from "moment";
import { TimeScale, toRoundedDateRange } from "../timeScaleDropdown";
import { AggregateStatistics } from "../statementsTable";
import { INTERNAL_APP_NAME_PREFIX } from "../recentExecutions/recentStatementUtils";

export type TableIndexStatsRequest =
  cockroach.server.serverpb.TableIndexStatsRequest;
export type TableIndexStatsResponse =
  cockroach.server.serverpb.TableIndexStatsResponse;
export type TableIndexStatsResponseWithKey = {
  indexStatsResponse: TableIndexStatsResponse;
  key: string;
};

type ResetIndexUsageStatsRequest =
  cockroach.server.serverpb.ResetIndexUsageStatsRequest;
type ResetIndexUsageStatsResponse =
  cockroach.server.serverpb.ResetIndexUsageStatsResponse;

// getIndexStats gets detailed stats about the current table's index usage statistics.
export const getIndexStats = (
  req: TableIndexStatsRequest,
): Promise<TableIndexStatsResponse> => {
  return fetchData(
    cockroach.server.serverpb.TableIndexStatsResponse,
    `/_status/databases/${req.database}/tables/${req.table}/indexstats`,
    null,
    null,
    "30M",
  );
};

// resetIndexStats refreshes all index usage stats for all tables.
export const resetIndexStats = (
  req: ResetIndexUsageStatsRequest,
): Promise<ResetIndexUsageStatsResponse> => {
  return fetchData(
    cockroach.server.serverpb.ResetIndexUsageStatsResponse,
    "/_status/resetindexusagestats",
    cockroach.server.serverpb.ResetIndexUsageStatsRequest,
    req,
    "30M",
  );
};

export type StatementsUsingIndexRequest = {
  table: string;
  index: string;
  database: string;
  start?: moment.Moment;
  end?: moment.Moment;
};

export function StatementsListRequestFromDetails(
  table: string,
  index: string,
  database: string,
  ts: TimeScale,
): StatementsUsingIndexRequest {
  if (ts === null) return { table, index, database };
  const [start, end] = toRoundedDateRange(ts);
  return { table, index, database, start, end };
}

export async function getStatementsUsingIndex({
  table,
  index,
  database,
  start,
  end,
}: StatementsUsingIndexRequest): Promise<AggregateStatistics[]> {
  const args: any = [`"${table}@${index}"`];
  let whereClause = "";
  if (start) {
    whereClause = `${whereClause} AND aggregated_ts >= '${start.toISOString()}'`;
  }
  if (end) {
    whereClause = `${whereClause} AND aggregated_ts <= '${end.toISOString()}'`;
  }

  const selectStatements = {
    sql: `SELECT * FROM system.statement_statistics 
            WHERE $1::jsonb <@ indexes_usage
                AND app_name NOT LIKE '${INTERNAL_APP_NAME_PREFIX}%' 
                ${whereClause}
            ORDER BY (statistics -> 'statistics' ->> 'cnt')::INT DESC
            LIMIT 20;`,
    arguments: args,
  };

  const req: SqlExecutionRequest = {
    execute: true,
    statements: [selectStatements],
    database: database,
    max_result_size: LARGE_RESULT_SIZE,
  };

  const result = await executeInternalSql<StatementRawFormat>(req);
  if (result.error) {
    throw new Error(
      `Error while retrieving list of statements: ${sqlApiErrorMessage(
        result.error.message,
      )}`,
    );
  }
  if (sqlResultsAreEmpty(result)) {
    return [];
  }

  return result.execution.txn_results[0].rows.map(s =>
    convertStatementRawFormatToAggregatedStatistics(s),
  );
}
