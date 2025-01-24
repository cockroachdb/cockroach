// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import Long from "long";
import moment from "moment-timezone";

import { fetchData } from "src/api/fetchData";
import {
  FixFingerprintHexValue,
  HexStringToInt64String,
  NumericStat,
  propsToQueryString,
  stringToTimestamp,
} from "src/util";

import { AggregateStatistics } from "../statementsTable";
const STATEMENTS_PATH = "_status/combinedstmts";
const STATEMENT_DETAILS_PATH = "_status/stmtdetails";

export type StatementsRequest =
  cockroach.server.serverpb.CombinedStatementsStatsRequest;
export type StatementDetailsRequest =
  cockroach.server.serverpb.StatementDetailsRequest;
export type StatementDetailsResponse =
  cockroach.server.serverpb.StatementDetailsResponse;
export type StatementDetailsResponseWithKey = {
  stmtResponse: StatementDetailsResponse;
  key: string;
};

export type SqlStatsResponse = cockroach.server.serverpb.IStatementsResponse;
export const SqlStatsSortOptions = cockroach.server.serverpb.StatsSortOptions;
export type SqlStatsSortType = cockroach.server.serverpb.StatsSortOptions;

const FetchStatsMode =
  cockroach.server.serverpb.CombinedStatementsStatsRequest.StatsType;

export type ErrorWithKey = {
  err: Error;
  key: string;
};

export const DEFAULT_STATS_REQ_OPTIONS = {
  limit: 100,
  sortStmt: SqlStatsSortOptions.PCT_RUNTIME,
  sortTxn: SqlStatsSortOptions.SERVICE_LAT,
};

// The required fields to create a stmts request.
type StmtReqFields = {
  limit: number;
  sort: SqlStatsSortType;
  start: moment.Moment;
  end: moment.Moment;
};

export function createCombinedStmtsRequest({
  limit,
  sort,
  start,
  end,
}: StmtReqFields): StatementsRequest {
  return new cockroach.server.serverpb.CombinedStatementsStatsRequest({
    start: Long.fromNumber(start.unix()),
    end: Long.fromNumber(end.unix()),
    limit: Long.fromNumber(limit ?? DEFAULT_STATS_REQ_OPTIONS.limit),
    fetch_mode:
      new cockroach.server.serverpb.CombinedStatementsStatsRequest.FetchMode({
        sort: sort,
      }),
  });
}

export const getCombinedStatements = (
  req: StatementsRequest,
): Promise<SqlStatsResponse> => {
  const queryStr = propsToQueryString({
    start: req.start.toInt(),
    end: req.end.toInt(),
    "fetch_mode.stats_type": FetchStatsMode.StmtStatsOnly,
    "fetch_mode.sort": req.fetch_mode?.sort,
    limit: req.limit?.toInt() ?? DEFAULT_STATS_REQ_OPTIONS.limit,
  });
  return fetchData(
    cockroach.server.serverpb.StatementsResponse,
    `${STATEMENTS_PATH}?${queryStr}`,
    null,
    null,
    "10M",
  );
};

export const getFlushedTxnStatsApi = (
  req: StatementsRequest,
): Promise<SqlStatsResponse> => {
  const queryStr = propsToQueryString({
    start: req.start?.toInt(),
    end: req.end?.toInt(),
    "fetch_mode.stats_type": FetchStatsMode.TxnStatsOnly,
    "fetch_mode.sort": req.fetch_mode?.sort,
    limit: req.limit?.toInt() ?? DEFAULT_STATS_REQ_OPTIONS.limit,
  });
  return fetchData(
    cockroach.server.serverpb.StatementsResponse,
    `${STATEMENTS_PATH}?${queryStr}`,
    null,
    null,
    "10M",
  );
};

export const getStatementDetails = (
  req: StatementDetailsRequest,
): Promise<cockroach.server.serverpb.StatementDetailsResponse> => {
  let queryStr = propsToQueryString({
    start: req.start.toInt(),
    end: req.end.toInt(),
  });
  for (const app of req.app_names) {
    queryStr += `&appNames=${app}`;
  }
  return fetchData(
    cockroach.server.serverpb.StatementDetailsResponse,
    `${STATEMENT_DETAILS_PATH}/${req.fingerprint_id}?${queryStr}`,
    null,
    null,
    "30M",
  );
};

export type StatementMetadata = {
  db: string;
  distsql: boolean;
  failed: boolean;
  fullScan: boolean;
  implicitTxn: boolean;
  query: string;
  querySummary: string;
  stmtType: string;
  vec: boolean;
};

type LatencyInfo = {
  max: number;
  min: number;
};

type Statistics = {
  bytesRead: NumericStat;
  cnt: Long;
  firstAttemptCnt: Long;
  idleLat: NumericStat;
  indexes: string[];
  lastErrorCode: string;
  lastExecAt: string;
  latencyInfo: LatencyInfo;
  maxRetries: Long;
  nodes: Long[];
  kvNodeIds: number[];
  numRows: NumericStat;
  ovhLat: NumericStat;
  parseLat: NumericStat;
  planGists: string[];
  planLat: NumericStat;
  rowsRead: NumericStat;
  rowsWritten: NumericStat;
  runLat: NumericStat;
  svcLat: NumericStat;
  regions: string[];
};

type ExecStats = {
  contentionTime: NumericStat;
  cnt: Long;
  maxDiskUsage: NumericStat;
  maxMemUsage: NumericStat;
  networkBytes: NumericStat;
  networkMsgs: NumericStat;
  cpuSQLNanos: NumericStat;
};

type StatementStatistics = {
  execution_statistics: ExecStats;
  index_recommendations: string[];
  statistics: Statistics;
};

export type StatementRawFormat = {
  aggregated_ts: number;
  fingerprint_id: string;
  transaction_fingerprint_id: string;
  plan_hash: string;
  app_name: string;
  node_id: number;
  agg_interval: number;
  metadata: StatementMetadata;
  statistics: StatementStatistics;
  index_recommendations: string[];
  indexes_usage: string[];
};

export function convertStatementRawFormatToAggregatedStatistics(
  s: StatementRawFormat,
): AggregateStatistics {
  return {
    applicationName: s.app_name,
    database: s.metadata.db,
    fullScan: s.metadata.fullScan,
    implicitTxn: s.metadata.implicitTxn,
    label: s.metadata.querySummary,
    summary: s.metadata.querySummary,
    aggregatedTs: s.aggregated_ts,
    aggregatedFingerprintID: HexStringToInt64String(s.fingerprint_id),
    aggregatedFingerprintHexID: FixFingerprintHexValue(s.fingerprint_id),
    stats: {
      exec_stats: {
        contention_time: s.statistics.execution_statistics.contentionTime,
        count: s.statistics.execution_statistics.cnt,
        max_disk_usage: s.statistics.execution_statistics.maxDiskUsage,
        max_mem_usage: s.statistics.execution_statistics.maxMemUsage,
        network_bytes: s.statistics.execution_statistics.networkBytes,
        network_messages: s.statistics.execution_statistics.networkMsgs,
        cpu_sql_nanos: s.statistics.execution_statistics.cpuSQLNanos,
      },
      bytes_read: s.statistics.statistics.bytesRead,
      count: s.statistics.statistics.cnt,
      first_attempt_count: s.statistics.statistics.firstAttemptCnt,
      idle_lat: s.statistics.statistics.idleLat,
      index_recommendations: s.statistics.index_recommendations,
      indexes: s.statistics.statistics.indexes,
      last_error_code: s.statistics.statistics.lastErrorCode,
      last_exec_timestamp: stringToTimestamp(
        s.statistics.statistics.lastExecAt,
      ),
      latency_info: {
        max: s.statistics.statistics.latencyInfo.max,
        min: s.statistics.statistics.latencyInfo.min,
      },
      max_retries: s.statistics.statistics.maxRetries,
      nodes: s.statistics.statistics.nodes,
      kv_node_ids: s.statistics.statistics.kvNodeIds,
      num_rows: s.statistics.statistics.numRows,
      overhead_lat: s.statistics.statistics.ovhLat,
      parse_lat: s.statistics.statistics.parseLat,
      plan_gists: s.statistics.statistics.planGists,
      plan_lat: s.statistics.statistics.planLat,
      rows_read: s.statistics.statistics.rowsRead,
      rows_written: s.statistics.statistics.rowsWritten,
      run_lat: s.statistics.statistics.runLat,
      service_lat: s.statistics.statistics.svcLat,
      sql_type: s.metadata.stmtType,
      regions: s.statistics.statistics.regions,
    },
  };
}
