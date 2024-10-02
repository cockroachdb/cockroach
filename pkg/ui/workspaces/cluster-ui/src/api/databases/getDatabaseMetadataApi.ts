// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import useSWR from "swr";

import { fetchDataJSON } from "src/api/fetchData";

import {
  APIV2ResponseWithPaginationState,
  SimplePaginationState,
} from "../types";

const DATABASES_API_V2 = "api/v2/database_metadata/";

export enum DatabaseSortOptions {
  NAME = "name",
  REPLICATION_SIZE = "replicationSize",
  RANGES = "ranges",
  TABLE_COUNT = "tableCount",
  LAST_UPDATED = "lastUpdated",
}

export type DatabaseMetadata = {
  db_id: number;
  db_name: string;
  size_bytes: number;
  table_count: number;
  store_ids: number[];
  last_updated: string;
};

export type DatabaseMetadataRequest = {
  name?: string;
  sortBy?: string;
  sortOrder?: string;
  pagination: SimplePaginationState;
  storeIds?: number[];
};

export type DatabaseMetadataResponse = APIV2ResponseWithPaginationState<
  DatabaseMetadata[]
>;

export const getDatabaseMetadata = async (req: DatabaseMetadataRequest) => {
  const urlParams = new URLSearchParams();
  if (req.name) {
    urlParams.append("name", req.name);
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
  if (req.storeIds?.length) {
    req.storeIds.forEach(id => urlParams.append("storeId", id.toString()));
  }

  return fetchDataJSON<DatabaseMetadataResponse, DatabaseMetadataRequest>(
    DATABASES_API_V2 + "?" + urlParams.toString(),
  );
};

const createKey = (req: DatabaseMetadataRequest) => {
  const { name, sortBy, sortOrder, pagination, storeIds } = req;
  return [
    "databaseMetadata",
    name,
    sortBy,
    sortOrder,
    pagination.pageSize,
    pagination.pageNum,
    storeIds.map(sid => sid.toString()).join(","),
  ].join("|");
};

export const useDatabaseMetadata = (req: DatabaseMetadataRequest) => {
  const { data, error, isLoading, mutate } = useSWR<DatabaseMetadataResponse>(
    createKey(req),
    () => getDatabaseMetadata(req),
    {
      revalidateOnFocus: false,
      revalidateOnReconnect: false,
    },
  );

  return {
    data,
    error,
    isLoading,
    refreshDatabases: mutate,
  };
};
