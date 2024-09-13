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
  LIVE_DATA = "liveData",
  COLUMNS = "columns",
  INDEXES = "indexes",
  LAST_UPDATED = "lastUpdated",
}

export type DatabaseMetadata = {
  db_id: number;
  db_name: string;
  size_bytes: number;
  table_count: number;
  store_ids: number[] | null;
  last_updated: string;
};

export type DatabaseMetadataRequest = {
  name?: string;
  sortBy?: string;
  sortOrder?: string;
  pagination: SimplePaginationState;
  storeId?: number;
};

export type DatabaseMetadataResponse =
  APIV2ResponseWithPaginationState<DatabaseMetadata>;

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
  if (req.storeId) {
    urlParams.append("storeId", req.storeId.toString());
  }

  return fetchDataJSON<DatabaseMetadataResponse, DatabaseMetadataRequest>(
    DATABASES_API_V2 + "?" + urlParams.toString(),
  );
};

const createKey = (req: DatabaseMetadataRequest) => {
  const { name, sortBy, sortOrder, pagination, storeId } = req;
  return [
    "databaseMetadata",
    name,
    sortBy,
    sortOrder,
    pagination.pageSize,
    pagination.pageNum,
    storeId,
  ].join("|");
};

export const useDatabaseMetadata = (req: DatabaseMetadataRequest) => {
  const { data, error, isLoading } = useSWR<DatabaseMetadataResponse>(
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
  };
};
