// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import useSWR from "swr";

import { StoreID } from "../../types/clusterTypes";
import { fetchDataJSON } from "../fetchData";
import {
  APIV2ResponseWithPaginationState,
  SimplePaginationState,
} from "../types";

const TABLE_METADATA_API_PATH = "api/v2/table_metadata/";

export enum TableSortOption {
  NAME = "name",
  REPLICATION_SIZE = "replicationSize",
  RANGES = "ranges",
  LIVE_DATA = "liveData",
  COLUMNS = "columns",
  INDEXES = "indexes",
  LAST_UPDATED = "lastUpdated",
}

export type TableMetadata = {
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
};

type TableMetadataResponse = APIV2ResponseWithPaginationState<TableMetadata[]>;

export type ListTableMetadataRequest = {
  dbId?: number;
  sortBy?: string;
  sortOrder?: "asc" | "desc";
  storeIds?: StoreID[];
  pagination: SimplePaginationState;
  name?: string;
};

async function getTableMetadata(
  req: ListTableMetadataRequest,
): Promise<TableMetadataResponse> {
  const urlParams = new URLSearchParams();
  if (req.dbId) {
    urlParams.append("dbId", req.dbId.toString());
  }
  if (req.sortBy) {
    urlParams.append("sortBy", req.sortBy);
  }
  if (req.sortOrder) {
    urlParams.append("sortOrder", req.sortOrder);
  }
  if (req.pagination.pageSize) {
    urlParams.append("pageSize", req.pagination.pageSize.toString());
  }
  if (req.pagination.pageNum) {
    urlParams.append("pageNum", req.pagination.pageNum.toString());
  }
  if (req.storeIds) {
    req.storeIds.forEach(storeID => {
      urlParams.append("storeId", storeID.toString());
    });
  }
  if (req.name) {
    urlParams.append("name", req.name);
  }
  return fetchDataJSON(TABLE_METADATA_API_PATH + "?" + urlParams.toString());
}

const createKey = (req: ListTableMetadataRequest) => {
  const { dbId, sortBy, sortOrder, pagination, storeIds, name } = req;
  return [
    "tableMetadata",
    dbId,
    sortBy,
    sortOrder,
    pagination.pageSize,
    pagination.pageNum,
    storeIds,
    name,
  ].join("|");
};

export const useTableMetadata = (req: ListTableMetadataRequest) => {
  const key = createKey(req);
  const { data, error, isLoading, mutate } = useSWR<TableMetadataResponse>(
    key,
    () => getTableMetadata(req),
  );

  return { data, error, isLoading, refreshTables: mutate };
};

type TableDetailsRequest = {
  tableId: number;
};

export type TableDetailsResponse = {
  metadata: TableMetadata;
  create_statement: string;
};
async function getTableDetails(
  req: TableDetailsRequest,
): Promise<TableDetailsResponse> {
  if (!req.tableId || Number.isNaN(req.tableId)) {
    throw new Error("Table ID is required");
  }
  return fetchDataJSON(`${TABLE_METADATA_API_PATH}${req.tableId}/`);
}

export const useTableDetails = (req: TableDetailsRequest) => {
  const { data, error, isLoading } = useSWR<TableDetailsResponse>(
    req.tableId.toString(),
    () => getTableDetails(req),
  );

  return { data, error: error, isLoading };
};
