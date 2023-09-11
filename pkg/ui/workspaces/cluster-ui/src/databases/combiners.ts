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
  buildIndexStatToRecommendationsMap,
  getNodesByRegionString,
  normalizePrivileges,
  normalizeRoles,
} from "./util";
import { DatabaseDetailsState } from "../store/databaseDetails";
import { createSelector } from "@reduxjs/toolkit";
import { TableDetailsState } from "../store/databaseTableDetails";
import { generateTableID, longToInt, TimestampToMoment } from "../util";
import { DatabaseDetailsPageDataTable } from "src/databaseDetailsPage";
import { DatabaseTablePageDataDetails, IndexStat } from "../databaseTablePage";
import { IndexStatsState } from "../store/indexStats";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { RecommendationType as RecType } from "../indexDetailsPage";
type IndexUsageStatistic =
  cockroach.server.serverpb.TableIndexStatsResponse.IExtendedCollectedIndexUsageStatistics;
const { RecommendationType } = cockroach.sql.IndexRecommendation;

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
  const nodes = dbStats?.replicaData.replicas || [];
  const nodesByRegionString = getNodesByRegionString(
    nodes,
    nodeRegionsByID,
    isTenant,
  );
  const numIndexRecommendations =
    dbStats?.indexStats.num_index_recommendations || 0;

  return {
    loading: !!dbDetails?.inFlight,
    loaded: !!dbDetails?.valid,
    requestError: dbDetails?.lastError,
    queryError: dbDetails?.data?.results?.error,
    name: database,
    spanStats: dbStats?.spanStats,
    tables: dbDetails?.data?.results.tablesResp,
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
    requestError: details?.lastError,
    queryError: details?.data?.results?.error,
    details: {
      schemaDetails: results?.schemaDetails,
      grants: {
        roles: normalizedRoles,
        privileges: normalizedPrivileges,
        error: results?.grantsResp?.error,
      },
      statsLastUpdated: results?.heuristicsDetails,
      indexStatRecs: results?.stats.indexStats,
      spanStats: results?.stats.spanStats,
      nodes: nodes,
      nodesByRegionString: getNodesByRegionString(nodes, nodeRegions, isTenant),
    },
  };
};

interface DerivedTablePageDetailsParams {
  details: TableDetailsState;
  nodeRegions: Record<string, string>;
  isTenant: boolean;
}

export const deriveTablePageDetailsMemoized = createSelector(
  (params: DerivedTablePageDetailsParams) => params.details,
  (params: DerivedTablePageDetailsParams) => params.nodeRegions,
  (params: DerivedTablePageDetailsParams) => params.isTenant,
  (details, nodeRegions, isTenant): DatabaseTablePageDataDetails => {
    const results = details?.data?.results;
    const grants = results?.grantsResp.grants || [];
    const normalizedGrants =
      grants.map(grant => ({
        user: grant.user,
        privileges: normalizePrivileges(grant.privileges),
      })) || [];
    const nodes = results?.stats.replicaData.nodeIDs || [];
    return {
      loading: !!details?.inFlight,
      loaded: !!details?.valid,
      lastError: details?.lastError,
      createStatement: results?.createStmtResp.create_statement || "",
      replicaCount: results?.stats.replicaData.replicaCount || 0,
      indexNames: results?.schemaDetails.indexes || [],
      grants: normalizedGrants,
      statsLastUpdated:
        results?.heuristicsDetails.stats_last_created_at || null,
      totalBytes: results?.stats.spanStats.total_bytes || 0,
      liveBytes: results?.stats.spanStats.live_bytes || 0,
      livePercentage: results?.stats.spanStats.live_percentage || 0,
      sizeInBytes: results?.stats.spanStats.approximate_disk_bytes || 0,
      rangeCount: results?.stats.spanStats.range_count || 0,
      nodesByRegionString: getNodesByRegionString(nodes, nodeRegions, isTenant),
    };
  },
);

interface DerivedIndexDetailsParams {
  database: string;
  table: string;
  indexUsageStats: Record<string, IndexStatsState>;
}

export const deriveIndexDetailsMemoized = createSelector(
  (params: DerivedIndexDetailsParams) => params.database,
  (params: DerivedIndexDetailsParams) => params.table,
  (params: DerivedIndexDetailsParams) => params.indexUsageStats,
  (database, table, indexUsageStats): IndexStat[] => {
    const indexStats = indexUsageStats[generateTableID(database, table)];
    const lastReset = TimestampToMoment(indexStats?.data?.last_reset);
    const stats = indexStats?.data?.statistics || [];
    const recommendations = indexStats?.data?.index_recommendations || [];
    const recsMap = buildIndexStatToRecommendationsMap(stats, recommendations);
    return stats.map(indexStat => {
      const indexRecs = recsMap[indexStat?.statistics.key.index_id] || [];
      return deriveIndexDetails(indexStat, lastReset, indexRecs);
    });
  },
);

const deriveIndexDetails = (
  indexStat: IndexUsageStatistic,
  lastReset: moment.Moment,
  recommendations: cockroach.sql.IIndexRecommendation[],
): IndexStat => {
  const lastRead = TimestampToMoment(indexStat.statistics?.stats?.last_read);
  let lastUsed, lastUsedType;
  if (indexStat.created_at !== null) {
    lastUsed = TimestampToMoment(indexStat.created_at);
    lastUsedType = "created";
  } else {
    lastUsed = lastReset;
    lastUsedType = "reset";
  }
  if (lastReset.isAfter(lastUsed)) {
    lastUsed = lastReset;
    lastUsedType = "reset";
  }
  if (lastRead.isAfter(lastUsed)) {
    lastUsed = lastRead;
    lastUsedType = "read";
  }
  const indexRecommendations = recommendations.map(indexRec => {
    let type: RecType = "Unknown";
    switch (RecommendationType[indexRec.type].toString()) {
      case "DROP_UNUSED":
        type = "DROP_UNUSED";
    }
    return {
      type: type,
      reason: indexRec.reason,
    };
  });
  return {
    indexName: indexStat.index_name,
    totalReads: longToInt(indexStat.statistics?.stats?.total_read_count),
    lastUsed: lastUsed,
    lastUsedType: lastUsedType,
    indexRecommendations,
  };
};
