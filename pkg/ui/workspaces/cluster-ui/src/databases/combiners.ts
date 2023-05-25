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
import { combineLoadingErrors, getNodesByRegionString } from "./util";
import { DatabaseDetailsState } from "../store/databaseDetails/databaseDetails.reducer";
import { createSelector } from "@reduxjs/toolkit";

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
