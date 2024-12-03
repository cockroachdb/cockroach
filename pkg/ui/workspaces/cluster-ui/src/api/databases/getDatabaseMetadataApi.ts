// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";

import { fetchDataJSON } from "src/api/fetchData";

import { StoreID } from "../../types/clusterTypes";
import { useSwrWithClusterId } from "../../util";
import { PaginationRequest, ResultsWithPagination } from "../types";

import {
  convertServerPaginationToClientPagination,
  DatabaseMetadataResponseServer,
  DatabaseMetadataServer,
} from "./serverTypes";

const DATABASES_API_V2 = "api/v2/database_metadata/";

export enum DatabaseSortOptions {
  NAME = "name",
  REPLICATION_SIZE = "replicationSize",
  RANGES = "ranges",
  TABLE_COUNT = "tableCount",
  LAST_UPDATED = "lastUpdated",
}

export type DatabaseMetadata = {
  dbId: number;
  dbName: string;
  sizeBytes: number;
  tableCount: number;
  storeIds: StoreID[];
  lastUpdated: moment.Moment | null;
};

export type DatabaseMetadataRequest = {
  name?: string;
  sortBy?: string;
  sortOrder?: string;
  pagination: PaginationRequest;
  storeIds?: number[];
};

type PaginatedDatabaseMetadata = ResultsWithPagination<DatabaseMetadata[]>;

const convertDatabaseMetadataFromServer = (
  d: DatabaseMetadataServer,
): DatabaseMetadata => {
  return {
    dbId: d.db_id,
    dbName: d.db_name,
    sizeBytes: d.size_bytes,
    tableCount: d.table_count,
    storeIds: d.store_ids?.map(sid => sid as StoreID) ?? [],
    lastUpdated: d.last_updated ? moment(d.last_updated) : null,
  };
};

export const getDatabaseMetadata = async (
  req: DatabaseMetadataRequest,
): Promise<PaginatedDatabaseMetadata> => {
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

  return fetchDataJSON<DatabaseMetadataResponseServer, null>(
    DATABASES_API_V2 + "?" + urlParams.toString(),
  ).then(resp => ({
    results: resp.results?.map(convertDatabaseMetadataFromServer) ?? [],
    pagination: convertServerPaginationToClientPagination(resp.pagination_info),
  }));
};

const createKey = (req: DatabaseMetadataRequest) => {
  const {
    name,
    sortBy,
    sortOrder,
    pagination: { pageSize, pageNum },
    storeIds,
  } = req;
  return {
    name: "databaseMetadata",
    dbName: name,
    sortBy,
    sortOrder,
    pageSize,
    pageNum,
    storeIds: storeIds,
  };
};

export const useDatabaseMetadata = (req: DatabaseMetadataRequest) => {
  const { data, error, isLoading, mutate } = useSwrWithClusterId(
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

type DatabaseMetadataByIDResponseServer = {
  metadata: DatabaseMetadataServer;
};

type DatabaseMetadataByIDResponse = {
  metadata: DatabaseMetadata;
};

const getDatabaseMetadataByID = async (
  dbID: number,
): Promise<DatabaseMetadataByIDResponse> => {
  return fetchDataJSON<DatabaseMetadataByIDResponseServer, null>(
    DATABASES_API_V2 + dbID + "/",
  ).then(resp => ({
    metadata: convertDatabaseMetadataFromServer(resp.metadata),
  }));
};

export const useDatabaseMetadataByID = (dbID: number) => {
  const { data, error, isLoading } =
    useSwrWithClusterId<DatabaseMetadataByIDResponse>(
      { name: "databaseMetadataByID", dbId: dbID },
      () => getDatabaseMetadataByID(dbID),
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
