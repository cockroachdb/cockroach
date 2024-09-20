// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
  store_ids: number[] | null;
  last_updated: string;
};

type TableMetadataResponse = APIV2ResponseWithPaginationState<TableMetadata[]>;

export type TableMetadataRequest = {
  dbID?: number;
  sortBy?: string;
  sortOrder?: "asc" | "desc";
  storeID?: StoreID[];
  pagination: SimplePaginationState;
  name?: string;
};

export async function getTableMetadata(
  req: TableMetadataRequest,
): Promise<TableMetadataResponse> {
  const urlParams = new URLSearchParams();
  if (req.dbID) {
    urlParams.append("dbId", req.dbID.toString());
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
  if (req.storeID) {
    req.storeID.forEach(storeID => {
      urlParams.append("storeId", storeID.toString());
    });
  }
  if (req.name) {
    urlParams.append("name", req.name);
  }
  return fetchDataJSON(TABLE_METADATA_API_PATH + "?" + urlParams.toString());
}

const createKey = (req: TableMetadataRequest) => {
  const { dbID, sortBy, sortOrder, pagination, storeID, name } = req;
  return [
    "tableMetadata",
    dbID,
    sortBy,
    sortOrder,
    pagination.pageSize,
    pagination.pageNum,
    storeID,
    name,
  ].join("|");
};

export const useTableMetadata = (req: TableMetadataRequest) => {
  const key = createKey(req);
  const { data, error, isLoading } = useSWR<TableMetadataResponse>(key, () =>
    getTableMetadata(req),
  );

  return { data, error, isLoading };
};
