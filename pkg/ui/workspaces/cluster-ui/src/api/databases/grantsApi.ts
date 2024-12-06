// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { useSwrImmutableWithClusterId } from "../../util";
import { fetchDataJSON } from "../fetchData";
import { ResultsWithPagination, PaginationRequest } from "../types";

import {
  convertServerPaginationToClientPagination,
  DatabaseGrantsResponseServer,
} from "./serverTypes";

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

const createKey = (req: DatabaseGrantsRequest) => {
  const {
    dbId,
    pagination: { pageSize, pageNum },
    sortBy,
    sortOrder,
  } = req;
  return { name: "databaseGrants", dbId, pageSize, pageNum, sortBy, sortOrder };
};

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

type DatabaseGrantsResponse = ResultsWithPagination<DatabaseGrant[]>;

const fetchDbGrants = (
  req: DatabaseGrantsRequest,
): Promise<DatabaseGrantsResponse> => {
  const path = createDbGrantsPath(req);
  return fetchDataJSON<DatabaseGrantsResponseServer, null>(path).then(resp => {
    return {
      results: resp.results.map(r => ({
        grantee: r.grantee,
        privilege: r.privilege,
      })),
      pagination: convertServerPaginationToClientPagination(
        resp.pagination_info,
      ),
    };
  });
};

export const useDatabaseGrantsImmutable = (req: DatabaseGrantsRequest) => {
  const { data, isLoading, error } = useSwrImmutableWithClusterId(
    createKey(req),
    () => fetchDbGrants(req),
  );

  return {
    databaseGrants: data?.results,
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
  const resp = await fetchDataJSON<DatabaseGrantsResponseServer, null>(path);
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
  const { data, isLoading, error } = useSwrImmutableWithClusterId(
    createTableGrantsPath(req),
    () => fetchTableGrants(req),
  );

  return {
    tableGrants: data?.results,
    isLoading,
    error: error,
  };
};
