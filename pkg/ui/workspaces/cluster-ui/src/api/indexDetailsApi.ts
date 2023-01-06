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
  SqlExecutionRequest,
  sqlResultsAreEmpty,
  StatementRawFormat,
} from "src/api";
import moment from "moment";
import { TimeScale, toDateRange } from "../timeScaleDropdown";
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
  const [start, end] = toDateRange(ts);
  return { table, index, database, start, end };
}

export function getStatementsUsingIndex({
  table,
  index,
  database,
  start,
  end,
}: StatementsUsingIndexRequest): Promise<AggregateStatistics[]> {
  const args: any = [`"${table}@${index}"`];
  let placeholder = 2;
  let whereClause = "";
  if (start) {
    args.push(start);
    whereClause = `${whereClause} AND aggregated_ts >= $${placeholder}`;
    placeholder++;
  }
  if (end) {
    args.push(end);
    whereClause = `${whereClause} AND aggregated_ts <= $${placeholder}`;
    placeholder++;
  }

  const selectStatements = {
    sql: `SELECT * FROM system.statement_statistics 
            WHERE $1::jsonb <@ indexes_usage
                AND app_name NOT LIKE '${INTERNAL_APP_NAME_PREFIX}%' 
                ${whereClause};`,
    arguments: args,
  };

  const req: SqlExecutionRequest = {
    execute: true,
    statements: [selectStatements],
    database: database,
  };

  return executeInternalSql<StatementRawFormat>(req).then(res => {
    if (res.error || sqlResultsAreEmpty(res)) {
      return [];
    }

    const statements: AggregateStatistics[] = [];
    res.execution.txn_results[0].rows.forEach(s => {
      statements.push(convertStatementRawFormatToAggregatedStatistics(s));
    });
    return statements;
  });
}
