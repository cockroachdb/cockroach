// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import moment from "moment-timezone";

import {
  convertStatementRawFormatToAggregatedStatistics,
  executeInternalSql,
  fetchData,
  formatApiResult,
  LARGE_RESULT_SIZE,
  SqlApiResponse,
  SqlExecutionRequest,
  sqlResultsAreEmpty,
  StatementRawFormat,
} from "src/api";

import { INTERNAL_APP_NAME_PREFIX } from "../activeExecutions/activeStatementUtils";
import { AggregateStatistics } from "../statementsTable";
import { TimeScale, toRoundedDateRange } from "../timeScaleDropdown";

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
    `_status/databases/${req.database}/tables/${req.table}/indexstats`,
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
    "_status/resetindexusagestats",
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
}: StatementsUsingIndexRequest): Promise<
  SqlApiResponse<AggregateStatistics[]>
> {
  const args = [`"${table}@${index}"`];
  let whereClause = "";
  if (start) {
    whereClause = `${whereClause} AND aggregated_ts >= '${start.toISOString()}'`;
  }
  if (end) {
    whereClause = `${whereClause} AND aggregated_ts <= '${end.toISOString()}'`;
  }

  const selectStatements = {
    sql: `SELECT * FROM crdb_internal.statement_statistics_persisted 
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
  if (sqlResultsAreEmpty(result)) {
    return formatApiResult<AggregateStatistics[]>(
      [],
      result.error,
      "retrieving list of statements per index",
    );
  }

  const rows = result.execution.txn_results[0].rows.map(s =>
    convertStatementRawFormatToAggregatedStatistics(s),
  );
  return formatApiResult<AggregateStatistics[]>(
    rows,
    result.error,
    "retrieving list of statements per index",
  );
}
