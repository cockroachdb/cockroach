// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { fetchDataJSON } from "src/api/fetchData";

import { useSwrWithClusterId } from "../util";

import { PaginationRequest, ResultsWithPagination } from "./types";

// ---------------------------------------------------------------------------
// API paths
// ---------------------------------------------------------------------------
const STATEMENTS_API = "api/v2/dbconsole/statements/";
const TRANSACTIONS_API = "api/v2/dbconsole/transactions/";

// ---------------------------------------------------------------------------
// Server response types (snake_case, matching Go JSON tags)
// ---------------------------------------------------------------------------
type PaginationInfoServer = {
  total_results: number;
  page_size: number;
  page_num: number;
};

type StatementRowServer = {
  fingerprint_id: string;
  execution_count: number;
  svc_lat_mean: number;
  svc_lat_stddev: number;
  cpu_sql_nanos_mean: number;
  cpu_sql_nanos_stddev: number;
  contention_time_mean: number;
  contention_time_stddev: number;
  kv_cpu_time_nanos_mean: number;
  kv_cpu_time_nanos_stddev: number;
  admission_wait_time_mean: number;
  admission_wait_time_stddev: number;
  rows_read_mean: number;
  rows_written_mean: number;
  bytes_read_mean: number;
  bytes_read_stddev: number;
  max_retries: number;
  app_name: string;
  pct_of_total_runtime: number;
  query: string;
  query_summary: string;
  database: string;
};

type TransactionRowServer = {
  fingerprint_id: string;
  execution_count: number;
  svc_lat_mean: number;
  svc_lat_stddev: number;
  cpu_sql_nanos_mean: number;
  cpu_sql_nanos_stddev: number;
  contention_time_mean: number;
  contention_time_stddev: number;
  kv_cpu_time_nanos_mean: number;
  kv_cpu_time_nanos_stddev: number;
  admission_wait_time_mean: number;
  admission_wait_time_stddev: number;
  rows_read_mean: number;
  rows_written_mean: number;
  bytes_read_mean: number;
  bytes_read_stddev: number;
  max_retries: number;
  commit_lat_mean: number;
  commit_lat_stddev: number;
  app_name: string;
  pct_of_total_runtime: number;
  query_summaries: string[];
};

type StatementsResponseServer = {
  results: StatementRowServer[];
  pagination_info: PaginationInfoServer;
};

type TransactionsResponseServer = {
  results: TransactionRowServer[];
  pagination_info: PaginationInfoServer;
};

// ---------------------------------------------------------------------------
// Client types (camelCase)
// ---------------------------------------------------------------------------
export type StatementRow = {
  fingerprintId: string;
  executionCount: number;
  svcLatMean: number;
  svcLatStddev: number;
  cpuSqlNanosMean: number;
  cpuSqlNanosStddev: number;
  contentionTimeMean: number;
  contentionTimeStddev: number;
  kvCpuTimeNanosMean: number;
  kvCpuTimeNanosStddev: number;
  admissionWaitTimeMean: number;
  admissionWaitTimeStddev: number;
  rowsReadMean: number;
  rowsWrittenMean: number;
  bytesReadMean: number;
  bytesReadStddev: number;
  maxRetries: number;
  appName: string;
  pctOfTotalRuntime: number;
  query: string;
  querySummary: string;
  database: string;
  key: string;
};

export type TransactionRow = {
  fingerprintId: string;
  executionCount: number;
  svcLatMean: number;
  svcLatStddev: number;
  cpuSqlNanosMean: number;
  cpuSqlNanosStddev: number;
  contentionTimeMean: number;
  contentionTimeStddev: number;
  kvCpuTimeNanosMean: number;
  kvCpuTimeNanosStddev: number;
  admissionWaitTimeMean: number;
  admissionWaitTimeStddev: number;
  rowsReadMean: number;
  rowsWrittenMean: number;
  bytesReadMean: number;
  bytesReadStddev: number;
  maxRetries: number;
  commitLatMean: number;
  commitLatStddev: number;
  appName: string;
  pctOfTotalRuntime: number;
  querySummaries: string[];
  key: string;
};

// ---------------------------------------------------------------------------
// Sort options (matching Go backend)
// ---------------------------------------------------------------------------
export enum StatementSortOptions {
  EXECUTION_COUNT = "execution_count",
  SVC_LAT_MEAN = "svc_lat_mean",
  CPU_SQL_NANOS_MEAN = "cpu_sql_nanos_mean",
  CONTENTION_TIME_MEAN = "contention_time_mean",
  KV_CPU_TIME_MEAN = "kv_cpu_time_mean",
  ADM_WAIT_TIME_MEAN = "adm_wait_time_mean",
  PCT_OF_TOTAL_RUNTIME = "pct_of_total_runtime",
  ROWS_READ_MEAN = "rows_read_mean",
  ROWS_WRITTEN_MEAN = "rows_written_mean",
  BYTES_READ_MEAN = "bytes_read_mean",
  MAX_RETRIES = "max_retries",
}

export enum TransactionSortOptions {
  EXECUTION_COUNT = "execution_count",
  SVC_LAT_MEAN = "svc_lat_mean",
  CPU_SQL_NANOS_MEAN = "cpu_sql_nanos_mean",
  CONTENTION_TIME_MEAN = "contention_time_mean",
  KV_CPU_TIME_MEAN = "kv_cpu_time_mean",
  ADM_WAIT_TIME_MEAN = "adm_wait_time_mean",
  PCT_OF_TOTAL_RUNTIME = "pct_of_total_runtime",
  ROWS_READ_MEAN = "rows_read_mean",
  ROWS_WRITTEN_MEAN = "rows_written_mean",
  BYTES_READ_MEAN = "bytes_read_mean",
  MAX_RETRIES = "max_retries",
  COMMIT_LAT_MEAN = "commit_lat_mean",
}

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------
export type SqlActivityStatementsRequest = {
  start?: number; // seconds since Unix epoch
  end?: number; // seconds since Unix epoch
  pagination: PaginationRequest;
  sortBy?: string;
  sortOrder?: string;
  search?: string;
  appName?: string;
  database?: string;
  excludeInternal?: boolean;
};

export type TransactionsRequest = {
  start?: number; // seconds since Unix epoch
  end?: number; // seconds since Unix epoch
  pagination: PaginationRequest;
  sortBy?: string;
  sortOrder?: string;
  appName?: string;
  excludeInternal?: boolean;
};

// ---------------------------------------------------------------------------
// Converters
// ---------------------------------------------------------------------------
const convertStatementRow = (s: StatementRowServer): StatementRow => ({
  fingerprintId: s.fingerprint_id,
  executionCount: s.execution_count,
  svcLatMean: s.svc_lat_mean,
  svcLatStddev: s.svc_lat_stddev,
  cpuSqlNanosMean: s.cpu_sql_nanos_mean,
  cpuSqlNanosStddev: s.cpu_sql_nanos_stddev,
  contentionTimeMean: s.contention_time_mean,
  contentionTimeStddev: s.contention_time_stddev,
  kvCpuTimeNanosMean: s.kv_cpu_time_nanos_mean,
  kvCpuTimeNanosStddev: s.kv_cpu_time_nanos_stddev,
  admissionWaitTimeMean: s.admission_wait_time_mean,
  admissionWaitTimeStddev: s.admission_wait_time_stddev,
  rowsReadMean: s.rows_read_mean,
  rowsWrittenMean: s.rows_written_mean,
  bytesReadMean: s.bytes_read_mean,
  bytesReadStddev: s.bytes_read_stddev,
  maxRetries: s.max_retries,
  appName: s.app_name,
  pctOfTotalRuntime: s.pct_of_total_runtime,
  query: s.query,
  querySummary: s.query_summary,
  database: s.database,
  key: `${s.fingerprint_id}-${s.app_name}`,
});

const convertTransactionRow = (t: TransactionRowServer): TransactionRow => ({
  fingerprintId: t.fingerprint_id,
  executionCount: t.execution_count,
  svcLatMean: t.svc_lat_mean,
  svcLatStddev: t.svc_lat_stddev,
  cpuSqlNanosMean: t.cpu_sql_nanos_mean,
  cpuSqlNanosStddev: t.cpu_sql_nanos_stddev,
  contentionTimeMean: t.contention_time_mean,
  contentionTimeStddev: t.contention_time_stddev,
  kvCpuTimeNanosMean: t.kv_cpu_time_nanos_mean,
  kvCpuTimeNanosStddev: t.kv_cpu_time_nanos_stddev,
  admissionWaitTimeMean: t.admission_wait_time_mean,
  admissionWaitTimeStddev: t.admission_wait_time_stddev,
  rowsReadMean: t.rows_read_mean,
  rowsWrittenMean: t.rows_written_mean,
  bytesReadMean: t.bytes_read_mean,
  bytesReadStddev: t.bytes_read_stddev,
  maxRetries: t.max_retries,
  commitLatMean: t.commit_lat_mean,
  commitLatStddev: t.commit_lat_stddev,
  appName: t.app_name,
  pctOfTotalRuntime: t.pct_of_total_runtime,
  querySummaries: t.query_summaries ?? [],
  key: `${t.fingerprint_id}-${t.app_name}`,
});

const convertPagination = (p: PaginationInfoServer) => ({
  pageNum: p?.page_num ?? 0,
  pageSize: p?.page_size ?? 0,
  totalResults: p?.total_results ?? 0,
});

// ---------------------------------------------------------------------------
// Fetch functions
// ---------------------------------------------------------------------------
const buildURLParams = (
  req: SqlActivityStatementsRequest | TransactionsRequest,
): string => {
  const p = new URLSearchParams();
  if (req.start) p.append("start", req.start.toString());
  if (req.end) p.append("end", req.end.toString());
  if (req.pagination.pageSize)
    p.append("pageSize", req.pagination.pageSize.toString());
  if (req.pagination.pageNum)
    p.append("pageNum", req.pagination.pageNum.toString());
  if (req.sortBy) p.append("sortBy", req.sortBy);
  if (req.sortOrder) p.append("sortOrder", req.sortOrder);
  if ("appName" in req && req.appName) p.append("appName", req.appName);
  if ("search" in req && req.search) p.append("search", req.search);
  if ("database" in req && req.database) p.append("database", req.database);
  if (req.excludeInternal === false) p.append("excludeInternal", "false");
  return p.toString();
};

export const getStatements = async (
  req: SqlActivityStatementsRequest,
): Promise<ResultsWithPagination<StatementRow[]>> => {
  const resp = await fetchDataJSON<StatementsResponseServer, null>(
    STATEMENTS_API + "?" + buildURLParams(req),
  );
  return {
    results: resp.results?.map(convertStatementRow) ?? [],
    pagination: convertPagination(resp.pagination_info),
  };
};

export const getTransactions = async (
  req: TransactionsRequest,
): Promise<ResultsWithPagination<TransactionRow[]>> => {
  const resp = await fetchDataJSON<TransactionsResponseServer, null>(
    TRANSACTIONS_API + "?" + buildURLParams(req),
  );
  return {
    results: resp.results?.map(convertTransactionRow) ?? [],
    pagination: convertPagination(resp.pagination_info),
  };
};

// ---------------------------------------------------------------------------
// SWR hooks
// ---------------------------------------------------------------------------
const createStatementsKey = (req: SqlActivityStatementsRequest) => ({
  name: "sqlActivityStatements",
  start: req.start,
  end: req.end,
  sortBy: req.sortBy,
  sortOrder: req.sortOrder,
  search: req.search,
  appName: req.appName,
  database: req.database,
  excludeInternal: req.excludeInternal,
  pageSize: req.pagination.pageSize,
  pageNum: req.pagination.pageNum,
});

export const useSqlActivityStatements = (req: SqlActivityStatementsRequest) => {
  const { data, error, isLoading, mutate } = useSwrWithClusterId(
    createStatementsKey(req),
    () => getStatements(req),
    { revalidateOnFocus: false, revalidateOnReconnect: false },
  );
  return { data, error, isLoading, refresh: mutate };
};

const createTransactionsKey = (req: TransactionsRequest) => ({
  name: "sqlActivityTransactions",
  start: req.start,
  end: req.end,
  sortBy: req.sortBy,
  sortOrder: req.sortOrder,
  appName: req.appName,
  excludeInternal: req.excludeInternal,
  pageSize: req.pagination.pageSize,
  pageNum: req.pagination.pageNum,
});

export const useSqlActivityTransactions = (req: TransactionsRequest) => {
  const { data, error, isLoading, mutate } = useSwrWithClusterId(
    createTransactionsKey(req),
    () => getTransactions(req),
    { revalidateOnFocus: false, revalidateOnReconnect: false },
  );
  return { data, error, isLoading, refresh: mutate };
};
