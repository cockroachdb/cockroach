// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { DatabasesListResponse, SqlExecutionErrorMessage } from "../api";
import { DatabasesPageDataDatabase } from "../databasesPage";
import {
  combineLoadingErrors,
  getNodesByRegionString,
  normalizePrivileges,
  normalizeRoles,
} from "./util";
import { DatabaseDetailsState } from "../store/databaseDetails";
import { createSelector } from "@reduxjs/toolkit";
import { TableDetailsState } from "../store/databaseTableDetails";
import { generateTableID } from "../util";
import { DatabaseDetailsPageDataTable } from "src/databaseDetailsPage";

interface DerivedDatabaseDetailsParams {
  dbListResp: DatabasesListResponse;
  databaseDetails: Record<string, DatabaseDetailsState>;
  nodeRegions: Record<string, string>;
  isTenant: boolean;
}

export const deriveDatabaseDetailsMemoized = createSelector(
  (params: DerivedDatabaseDetailsParams) => params.dbListResp,
  (params: DerivedDatabaseDetailsParams) => params.databaseDetails,
  (params: DerivedDatabaseDetailsParams) => params.nodeRegions,
  (params: DerivedDatabaseDetailsParams) => params.isTenant,
  (
    dbListResp,
    databaseDetails,
    nodeRegions,
    isTenant,
  ): DatabasesPageDataDatabase[] => {
    const databases = dbListResp?.databases ?? [];
    return databases.map(dbName => {
      const dbDetails = databaseDetails[dbName];
      return deriveDatabaseDetails(
        dbName,
        dbDetails,
        dbListResp.error,
        nodeRegions,
        isTenant,
      );
    });
  },
);

const deriveDatabaseDetails = (
  database: string,
  dbDetails: DatabaseDetailsState,
  dbListError: SqlExecutionErrorMessage,
  nodeRegionsByID: Record<string, string>,
  isTenant: boolean,
): DatabasesPageDataDatabase => {
  const dbStats = dbDetails?.data?.results.stats;
  const sizeInBytes = dbStats?.spanStats?.approximate_disk_bytes || 0;
  const rangeCount = dbStats?.spanStats.range_count || 0;
  const nodes = dbStats?.replicaData.replicas || [];
  const nodesByRegionString = getNodesByRegionString(
    nodes,
    nodeRegionsByID,
    isTenant,
  );
  const numIndexRecommendations =
    dbStats?.indexStats.num_index_recommendations || 0;

  const combinedErr = combineLoadingErrors(
    dbDetails?.lastError,
    dbDetails?.data?.maxSizeReached,
    dbListError?.message,
  );

  return {
    loading: !!dbDetails?.inFlight,
    loaded: !!dbDetails?.valid,
    lastError: combinedErr,
    name: database,
    sizeInBytes: sizeInBytes,
    tableCount: dbDetails?.data?.results.tablesResp.tables?.length || 0,
    rangeCount: rangeCount,
    nodes: nodes,
    nodesByRegionString,
    numIndexRecommendations,
  };
};

interface DerivedTableDetailsParams {
  dbName: string;
  tables: string[];
  tableDetails: Record<string, TableDetailsState>;
  nodeRegions: Record<string, string>;
  isTenant: boolean;
}

export const deriveTableDetailsMemoized = createSelector(
  (params: DerivedTableDetailsParams) => params.dbName,
  (params: DerivedTableDetailsParams) => params.tables,
  (params: DerivedTableDetailsParams) => params.tableDetails,
  (params: DerivedTableDetailsParams) => params.nodeRegions,
  (params: DerivedTableDetailsParams) => params.isTenant,
  (
    dbName,
    tables,
    tableDetails,
    nodeRegions,
    isTenant,
  ): DatabaseDetailsPageDataTable[] => {
    tables = tables || [];
    return tables.map(table => {
      const tableID = generateTableID(dbName, table);
      const details = tableDetails[tableID];
      return deriveDatabaseTableDetails(table, details, nodeRegions, isTenant);
    });
  },
);

const deriveDatabaseTableDetails = (
  table: string,
  details: TableDetailsState,
  nodeRegions: Record<string, string>,
  isTenant: boolean,
): DatabaseDetailsPageDataTable => {
  const results = details?.data?.results;
  const grants = results?.grantsResp.grants ?? [];
  const normalizedRoles = normalizeRoles(grants.map(grant => grant.user));
  const normalizedPrivileges = normalizePrivileges(
    [].concat(...grants.map(grant => grant.privileges)),
  );
  const nodes = results?.stats.replicaData.nodeIDs || [];
  return {
    name: table,
    loading: !!details?.inFlight,
    loaded: !!details?.valid,
    lastError: details?.lastError,
    details: {
      columnCount: results?.schemaDetails.columns?.length || 0,
      indexCount: results?.schemaDetails.indexes.length || 0,
      userCount: normalizedRoles.length,
      roles: normalizedRoles,
      grants: normalizedPrivileges,
      statsLastUpdated:
        results?.heuristicsDetails.stats_last_created_at || null,
      hasIndexRecommendations:
        results?.stats.indexStats.has_index_recommendations || false,
      totalBytes: results?.stats?.spanStats.total_bytes || 0,
      liveBytes: results?.stats?.spanStats.live_bytes || 0,
      livePercentage: results?.stats?.spanStats.live_percentage || 0,
      replicationSizeInBytes:
        results?.stats?.spanStats.approximate_disk_bytes || 0,
      nodes: nodes,
      rangeCount: results?.stats?.spanStats.range_count || 0,
      nodesByRegionString: getNodesByRegionString(nodes, nodeRegions, isTenant),
    },
  };
};
