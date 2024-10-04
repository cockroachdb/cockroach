// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import useSWRImmutable from "swr/immutable";

import { fetchDataJSON } from "../fetchData";
import {
  APIV2ResponseWithPaginationState,
  ResultsWithPagination,
  PaginationRequest,
} from "../types";

export type DatabaseGrant = {
  grantee: string;
  privilege: string;
};

export enum GrantsSortOptions {
  GRANTEE = "grantee",
  PRIVILEGE = "privilege",
}

type DatabaseGrantsRequest = {
  dbId: number;
  sortBy?: GrantsSortOptions;
  sortOrder?: "asc" | "desc";
  pagination?: PaginationRequest;
};

export type DatabaseGrantsResponse = APIV2ResponseWithPaginationState<
  DatabaseGrant[]
>;

const createDbGrantsPath = (req: DatabaseGrantsRequest): string => {
  const { dbId, pagination, sortBy, sortOrder } = req;
  const urlParams = new URLSearchParams();
  if (pagination?.pageNum) {
    urlParams.append("pageNum", pagination.pageNum.toString());
  }
  if (pagination?.pageSize) {
    urlParams.append("pageSize", pagination.pageSize.toString());
  }
  if (sortBy) {
    urlParams.append("sortBy", sortBy);
  }
  if (sortOrder) {
    urlParams.append("sortOrder", sortOrder);
  }
  return `api/v2/grants/databases/${dbId}/?` + urlParams.toString();
};

const fetchDbGrants = (
  req: DatabaseGrantsRequest,
): Promise<DatabaseGrantsResponse> => {
  const path = createDbGrantsPath(req);
  return fetchDataJSON(path);
};

export const useDatabaseGrantsImmutable = (req: DatabaseGrantsRequest) => {
  const { data, isLoading, error } = useSWRImmutable(
    createDbGrantsPath(req),
    () => fetchDbGrants(req),
  );

  return {
    databaseGrants: data?.results,
    pagination: data?.pagination_info,
    isLoading,
    error: error,
  };
};

type TableGrantsRequest = {
  tableId: number;
  sortBy?: GrantsSortOptions;
  sortOrder?: "asc" | "desc";
  pagination?: PaginationRequest;
};

export type TableGrantsResponse = ResultsWithPagination<DatabaseGrant[]>;

const createTableGrantsPath = (req: TableGrantsRequest): string => {
  const { tableId, pagination, sortBy, sortOrder } = req;
  const urlParams = new URLSearchParams();
  if (pagination?.pageNum) {
    urlParams.append("pageNum", pagination.pageNum.toString());
  }
  if (pagination?.pageSize) {
    urlParams.append("pageSize", pagination.pageSize.toString());
  }
  if (sortBy) {
    urlParams.append("sortBy", sortBy);
  }
  if (sortOrder) {
    urlParams.append("sortOrder", sortOrder);
  }
  return `api/v2/grants/tables/${tableId}/?` + urlParams.toString();
};

const fetchTableGrants = async (
  req: TableGrantsRequest,
): Promise<TableGrantsResponse> => {
  const path = createTableGrantsPath(req);
  const resp = await fetchDataJSON<
    APIV2ResponseWithPaginationState<DatabaseGrant[]>,
    null
  >(path);
  return {
    results: resp.results,
    pagination: {
      pageSize: resp.pagination_info?.page_size ?? 0,
      pageNum: resp.pagination_info?.page_num ?? 0,
      totalResults: resp.pagination_info?.total_results ?? 0,
    },
  };
};

export const useTableGrantsImmutable = (req: TableGrantsRequest) => {
  const { data, isLoading, error } = useSWRImmutable(
    createTableGrantsPath(req),
    () => fetchTableGrants(req),
  );

  return {
    tableGrants: data?.results,
    isLoading,
    error: error,
  };
};
