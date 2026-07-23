// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// ------------------------------------------------------------------------------------
// This file contains the types for the server api responses from /api/v2/ endpoints.
// ------------------------------------------------------------------------------------

import { PaginationState } from "../types";

// ------------------------------------------------------------------------------------
// Response with pagination.
// ------------------------------------------------------------------------------------
type APIV2PaginationResponse = {
  total_results: number;
  page_size: number;
  page_num: number;
};

type APIV2ResponseWithPaginationState<T> = {
  results: T[];
  pagination_info: APIV2PaginationResponse;
};

export const convertServerPaginationToClientPagination = (
  pagination: APIV2PaginationResponse,
): PaginationState => {
  return {
    pageNum: pagination?.page_num ?? 0,
    pageSize: pagination?.page_size ?? 0,
    totalResults: pagination?.total_results ?? 0,
  };
};

// ------------------------------------------------------------------------------------
// /api/v2/table_metadata/ response.
// ------------------------------------------------------------------------------------
export type TableMetadataServer = {
  db_id: number;
  db_name: string;
  table_id: number;
  schema_name: string;
  table_name: string;
  replication_size_bytes: number;
  range_count: number;
  column_count: number;
  index_count: number;
  percent_live_data: number;
  total_live_data_bytes: number;
  total_data_bytes: number;
  store_ids: number[];
  last_updated: string;
  last_update_error: string | null;
  auto_stats_enabled: boolean;
  // Optimizer stats.
  stats_last_updated: string | null;
  replica_count: number;
};

export type TableMetadataResponseServer =
  APIV2ResponseWithPaginationState<TableMetadataServer>;

// ------------------------------------------------------------------------------------
// /api/v2/table_metadata/:tableId/ response.
// ------------------------------------------------------------------------------------
export type TableDetailsResponseServer = {
  metadata: TableMetadataServer;
  create_statement: string;
};

// ------------------------------------------------------------------------------------
// /api/v2/database_metadata/ response.
// ------------------------------------------------------------------------------------
export type DatabaseMetadataServer = {
  db_id: number;
  db_name: string;
  size_bytes: number;
  table_count: number;
  store_ids: number[];
  last_updated: string;
};

export type DatabaseMetadataResponseServer =
  APIV2ResponseWithPaginationState<DatabaseMetadataServer>;

// ------------------------------------------------------------------------------------
// /api/v2/grants/tabless/:tableId/ response.
// /api/v2/grants/databases/:dbId/ response.
// ------------------------------------------------------------------------------------

export type DatabaseGrantServer = {
  grantee: string;
  privilege: string;
};

export type DatabaseGrantsResponseServer =
  APIV2ResponseWithPaginationState<DatabaseGrantServer>;

// ------------------------------------------------------------------------------------
// /api/v2/updatejob/ response.
// ------------------------------------------------------------------------------------

export type TableMetaUpdateJobResponseServer = {
  current_status: string;
  progress: number;
  last_start_time: string | null;
  last_completed_time: string | null;
  last_updated_time: string | null;
  data_valid_duration: number;
  automatic_updates_enabled: boolean;
};

export type TriggerTableMetaUpdateJobResponseServer = {
  job_triggered: boolean;
  message: string;
};
