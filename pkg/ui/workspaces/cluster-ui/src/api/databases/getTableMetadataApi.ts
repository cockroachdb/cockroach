// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";

import { StoreID } from "../../types/clusterTypes";
import { useSwrWithClusterId } from "../../util";
import { fetchDataJSON } from "../fetchData";
import { PaginationRequest, ResultsWithPagination } from "../types";

import {
  convertServerPaginationToClientPagination,
  TableDetailsResponseServer,
  TableMetadataResponseServer,
  TableMetadataServer,
} from "./serverTypes";

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

// Client type.
export type TableMetadata = {
  dbId: number;
  dbName: string;
  tableId: number;
  schemaName: string;
  tableName: string;
  replicationSizeBytes: number;
  rangeCount: number;
  columnCount: number;
  indexCount: number;
  percentLiveData: number;
  totalLiveDataBytes: number;
  totalDataBytes: number;
  storeIds: StoreID[];
  lastUpdated: moment.Moment;
  lastUpdateError: string | null;
  autoStatsEnabled: boolean;
  // Optimizer stats.
  statsLastUpdated: moment.Moment | null;
  replicaCount: number;
};

export type ListTableMetadataRequest = {
  dbId?: number;
  sortBy?: string;
  sortOrder?: "asc" | "desc";
  storeIds?: StoreID[];
  pagination: PaginationRequest;
  name?: string;
};

type TableMetadataResponse = ResultsWithPagination<TableMetadata[]>;

const convertTableMetadataFromServer = (
  resp: TableMetadataServer,
): TableMetadata => {
  return {
    dbId: resp.db_id ?? 0,
    dbName: resp.db_name ?? "",
    tableId: resp.table_id ?? 0,
    schemaName: resp.schema_name ?? "",
    tableName: resp.table_name ?? "",
    replicationSizeBytes: resp.replication_size_bytes ?? 0,
    rangeCount: resp.range_count ?? 0,
    columnCount: resp.column_count ?? 0,
    indexCount: resp.index_count ?? 0,
    percentLiveData: resp.percent_live_data ?? 0,
    totalLiveDataBytes: resp.total_live_data_bytes ?? 0,
    totalDataBytes: resp.total_data_bytes ?? 0,
    storeIds: resp.store_ids?.map(storeID => storeID as StoreID) ?? [],
    lastUpdated: moment(resp.last_updated ?? ""),
    lastUpdateError: resp.last_update_error,
    autoStatsEnabled: resp.auto_stats_enabled,
    statsLastUpdated: resp.stats_last_updated
      ? moment(resp.stats_last_updated)
      : null,
    replicaCount: resp.replica_count ?? 0,
  };
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
  return fetchDataJSON<TableMetadataResponseServer, null>(
    TABLE_METADATA_API_PATH + "?" + urlParams.toString(),
  ).then(resp => {
    return {
      results: resp.results?.map(convertTableMetadataFromServer) ?? [],
      pagination: convertServerPaginationToClientPagination(
        resp.pagination_info,
      ),
    };
  });
}

const createKey = (req: ListTableMetadataRequest) => {
  const {
    dbId,
    sortBy,
    sortOrder,
    pagination: { pageSize, pageNum },
    storeIds,
    name,
  } = req;
  return {
    name: "tableMetadata",
    tableName: name,
    dbId,
    sortBy,
    sortOrder,
    pageSize,
    pageNum,
    storeIds,
  };
};

export const useTableMetadata = (req: ListTableMetadataRequest) => {
  const { data, error, isLoading, mutate } =
    useSwrWithClusterId<TableMetadataResponse>(
      createKey(req),
      () => getTableMetadata(req),
      {
        revalidateOnFocus: false,
        revalidateOnReconnect: false,
      },
    );

  return { data, error, isLoading, refreshTables: mutate };
};

// Point lookup - table details.

type TableDetailsRequest = {
  tableId: number;
};

export type TableDetails = {
  metadata: TableMetadata;
  createStatement: string;
};

async function getTableDetails(
  req: TableDetailsRequest,
): Promise<TableDetails> {
  if (!req.tableId || Number.isNaN(req.tableId)) {
    throw new Error("Table ID is required");
  }
  return fetchDataJSON<TableDetailsResponseServer, null>(
    `${TABLE_METADATA_API_PATH}${req.tableId}/`,
  ).then((resp: TableDetailsResponseServer) => {
    return {
      metadata: convertTableMetadataFromServer(resp.metadata),
      createStatement: resp.create_statement,
    };
  });
}

export const useTableDetails = (req: TableDetailsRequest) => {
  const { data, error, isLoading } = useSwrWithClusterId<TableDetails>(
    { name: "tableDetails", tableId: req.tableId.toString() },
    () => getTableDetails(req),
  );

  return { data, error: error, isLoading };
};
