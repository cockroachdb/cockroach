// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { fetchDataJSON } from "src/api/fetchData";

import { useSwrWithClusterId } from "../../util";
import {
  PaginationRequest,
  PaginationState,
  ResultsWithPagination,
} from "../types";

const STATEMENT_ACTIVITIES_API = "api/v2/sql_activity/statements/";

// Server response types matching the backend AggregatedStatementActivity struct
type StatementActivityServer = {
  fingerprint_id: string;
  query: string;
  query_summary: string;
  execution_count: number;
  service_latency_mean: number;
  service_latency_stddev: number;
  sql_cpu_mean_nanos: number;
  sql_cpu_stddev: number;
  contention_time_mean: number;
  contention_time_stddev: number;
  kv_cpu_mean_nanos: number;
  kv_cpu_stddev: number;
  admission_wait_time_mean: number;
  admission_wait_time_stddev: number;
  pct_of_total_runtime: number;
};

type StatementActivitiesResponseServer = {
  results: StatementActivityServer[];
  pagination_info: {
    total_results: number;
    page_size: number;
    page_num: number;
  };
};

// Client-side types with camelCase naming
export type StatementActivity = {
  fingerprintId: string;
  query: string;
  querySummary: string;
  executionCount: number;
  serviceLatencyMean: number;
  serviceLatencyStdDev: number;
  sqlCpuMeanNanos: number;
  sqlCpuStdDev: number;
  contentionTimeMean: number;
  contentionTimeStdDev: number;
  kvCpuMeanNanos: number;
  kvCpuStdDev: number;
  admissionWaitTimeMean: number;
  admissionWaitTimeStdDev: number;
  pctOfTotalRuntime: number;
};

// Sort options that map to backend column names
export enum StatementActivitySortCol {
  EXECUTION_COUNT = "cnt",
  PCT_RUNTIME = "pct_of_total_runtime",
  SERVICE_LATENCY = "svc_lat_mean",
  SQL_CPU = "cpu_sql_nanos_mean",
  CONTENTION = "contention_time_mean",
  KV_CPU = "kv_cpu_time_nanos_mean",
  ADMISSION_WAIT = "admission_wait_time_mean",
}

export type StatementActivitiesRequest = {
  pagination: PaginationRequest;
  sortBy?: StatementActivitySortCol;
  sortOrder?: "asc" | "desc";
  topKCount?: number;
  topKCol?: StatementActivitySortCol;
  startTime?: number; // Unix timestamp in seconds
  endTime?: number; // Unix timestamp in seconds
};

type PaginatedStatementActivities = ResultsWithPagination<StatementActivity[]>;

const convertFromServer = (s: StatementActivityServer): StatementActivity => {
  return {
    fingerprintId: s.fingerprint_id,
    query: s.query ?? "",
    querySummary: s.query_summary ?? "",
    executionCount: s.execution_count ?? 0,
    serviceLatencyMean: s.service_latency_mean ?? 0,
    serviceLatencyStdDev: s.service_latency_stddev ?? 0,
    sqlCpuMeanNanos: s.sql_cpu_mean_nanos ?? 0,
    sqlCpuStdDev: s.sql_cpu_stddev ?? 0,
    contentionTimeMean: s.contention_time_mean ?? 0,
    contentionTimeStdDev: s.contention_time_stddev ?? 0,
    kvCpuMeanNanos: s.kv_cpu_mean_nanos ?? 0,
    kvCpuStdDev: s.kv_cpu_stddev ?? 0,
    admissionWaitTimeMean: s.admission_wait_time_mean ?? 0,
    admissionWaitTimeStdDev: s.admission_wait_time_stddev ?? 0,
    pctOfTotalRuntime: s.pct_of_total_runtime ?? 0,
  };
};

const convertServerPaginationToClientPagination = (
  pagination: StatementActivitiesResponseServer["pagination_info"],
): PaginationState => {
  return {
    pageNum: pagination?.page_num ?? 0,
    pageSize: pagination?.page_size ?? 0,
    totalResults: pagination?.total_results ?? 0,
  };
};

export const getStatementActivities = async (
  req: StatementActivitiesRequest,
): Promise<PaginatedStatementActivities> => {
  const urlParams = new URLSearchParams();

  // TopK parameters
  const topKCount = req.topKCount ?? 100;
  const topKCol = req.topKCol ?? StatementActivitySortCol.EXECUTION_COUNT;
  urlParams.append("topKCount", topKCount.toString());
  urlParams.append("topKCol", topKCol);

  // Time range parameters
  if (req.startTime !== undefined) {
    urlParams.append("startTime", req.startTime.toString());
  }
  if (req.endTime !== undefined) {
    urlParams.append("endTime", req.endTime.toString());
  }

  // Sorting parameters
  if (req.sortBy) {
    urlParams.append("sortBy", req.sortBy);
  }
  if (req.sortOrder) {
    urlParams.append("sortOrder", req.sortOrder.toUpperCase());
  }

  if (req.pagination.pageSize) {
    urlParams.append("pageSize", req.pagination.pageSize.toString());
  }
  if (req.pagination.pageNum) {
    urlParams.append("pageNum", req.pagination.pageNum.toString());
  }

  return fetchDataJSON<StatementActivitiesResponseServer, null>(
    STATEMENT_ACTIVITIES_API + "?" + urlParams.toString(),
  ).then(resp => ({
    results: resp.results?.map(convertFromServer) ?? [],
    pagination: convertServerPaginationToClientPagination(resp.pagination_info),
  }));
};

const createKey = (req: StatementActivitiesRequest) => {
  const {
    pagination: { pageSize, pageNum },
    sortBy,
    sortOrder,
    topKCount,
    topKCol,
    startTime,
    endTime,
  } = req;
  return {
    name: "statementActivities",
    pageSize,
    pageNum,
    sortBy,
    sortOrder,
    topKCount,
    topKCol,
    startTime,
    endTime,
  };
};

export const useStatementActivities = (req: StatementActivitiesRequest) => {
  const { data, error, isLoading, mutate } = useSwrWithClusterId(
    createKey(req),
    () => getStatementActivities(req),
    {
      revalidateOnFocus: false,
      revalidateOnReconnect: false,
    },
  );

  return {
    data,
    error,
    isLoading,
    refresh: mutate,
  };
};
