// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import {
  DatabaseDetailsResponse,
  DatabasesListResponse,
  SqlApiResponse,
  SqlExecutionErrorMessage,
  TableDetailsResponse,
} from "../api";
import { DatabasesPageDataDatabase } from "../databasesPage";
import {
  buildIndexStatToRecommendationsMap,
  combineLoadingErrors,
  generateTableID,
  getNodesByRegionString,
  longToInt,
  normalizePrivileges,
  normalizeRoles,
  TimestampToMoment,
} from "../util";
import { DatabaseDetailsPageDataTable } from "../databaseDetailsPage";
import { DatabaseTablePageDataDetails, IndexStat } from "../databaseTablePage";
import { RecommendationType as RecType } from "../indexDetailsPage";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import moment from "moment";
import { TableIndexStatsResponse } from "../api/indexDetailsApi";

type IndexUsageStatistic =
  cockroach.server.serverpb.TableIndexStatsResponse.IExtendedCollectedIndexUsageStatistics;
const { RecommendationType } = cockroach.sql.IndexRecommendation;

// DatabaseDetailsState is defined as shared state between cluster-ui and db-console.
// Currently, this interface mimics cluster-ui's DatabaseDetailsState type but we
// redefine it here as an interface to decouple.
export interface DatabaseDetailsState {
  data?: SqlApiResponse<DatabaseDetailsResponse>;
  lastError?: Error;
  valid: boolean;
  inFlight: boolean;
}

export const selectDatabases = (
  dbListResp: DatabasesListResponse,
  databaseDetails: Record<string, DatabaseDetailsState>,
  nodeRegions: Record<string, string>,
  isTenant: boolean,
): DatabasesPageDataDatabase[] => {
  const databases = dbListResp?.databases || [];
  return databases.map(dbName => {
    const dbDetails = databaseDetails[dbName];
    return databaseCombiner(
      dbName,
      dbDetails,
      dbListResp.error,
      nodeRegions,
      isTenant,
    );
  });
};

const databaseCombiner = (
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

// TableDetailsState is defined as shared state between cluster-ui and db-console.
// Currently, this interface mimics cluster-ui's TableDetailsState type but we
// redefine it here as an interface to decouple.
export interface TableDetailsState {
  data?: SqlApiResponse<TableDetailsResponse>;
  lastError?: Error;
  valid: boolean;
  inFlight: boolean;
}

export const selectTables = (
  database: string,
  tables: string[],
  tableDetails: Record<string, TableDetailsState>,
  nodeRegions: Record<string, string>,
  isTenant: boolean,
): DatabaseDetailsPageDataTable[] => {
  tables = tables || [];
  return tables.map(table => {
    const tableID = generateTableID(database, table);
    const details = tableDetails[tableID];
    return tableCombiner(table, details, nodeRegions, isTenant);
  });
};

const tableCombiner = (
  table: string,
  details: TableDetailsState,
  nodeRegions: Record<string, string>,
  isTenant: boolean,
): DatabaseDetailsPageDataTable => {
  const stats = details?.data?.results.stats;
  const grants = details?.data?.results.grantsResp.grants || [];
  const normalizedRoles = normalizeRoles(grants.map(grant => grant["user"]));
  const normalizedPrivileges = normalizePrivileges(
    [].concat(...grants.map(grant => grant["privileges"])),
  );
  const nodes = details?.data?.results.stats.replicaData.nodeIDs || [];
  const numIndexes = details?.data?.results.schemaDetails.indexes.length;
  return {
    name: table,
    loading: !!details?.inFlight,
    loaded: !!details?.valid,
    lastError: details?.lastError,
    details: {
      columnCount: details?.data?.results.schemaDetails.columns?.length || 0,
      indexCount: numIndexes,
      userCount: normalizedRoles.length,
      roles: normalizedRoles,
      grants: normalizedPrivileges,
      statsLastUpdated:
        details?.data?.results.heuristicsDetails.stats_last_created_at || null,
      hasIndexRecommendations:
        details?.data?.results.stats.indexStats.has_index_recommendations ||
        false,
      totalBytes: stats?.spanStats.total_bytes || 0,
      liveBytes: stats?.spanStats.live_bytes || 0,
      livePercentage: stats?.spanStats.live_percentage || 0,
      replicationSizeInBytes: stats?.spanStats.approximate_disk_bytes || 0,
      nodes: nodes,
      rangeCount: stats?.spanStats.range_count || 0,
      nodesByRegionString: getNodesByRegionString(nodes, nodeRegions, isTenant),
    },
  };
};

export const selectTablePageDataDetails = (
  details: TableDetailsState,
  nodeRegions: Record<string, string>,
  isTenant: boolean,
): DatabaseTablePageDataDetails => {
  const grants = details?.data?.results.grantsResp.grants || [];
  const normalizedGrants =
    grants.map(grant => ({
      user: grant.user,
      privileges: normalizePrivileges(grant.privileges),
    })) || [];
  const nodes = details?.data?.results.stats.replicaData.nodeIDs || [];
  return {
    loading: !!details?.inFlight,
    loaded: !!details?.valid,
    lastError: details?.lastError,
    createStatement:
      details?.data?.results.createStmtResp.create_statement || "",
    replicaCount: details?.data?.results.stats.replicaData.replicaCount || 0,
    indexNames: details?.data?.results.schemaDetails.indexes || [],
    grants: normalizedGrants,
    statsLastUpdated:
      details?.data?.results.heuristicsDetails.stats_last_created_at || null,
    totalBytes: details?.data?.results.stats.spanStats.total_bytes || 0,
    liveBytes: details?.data?.results.stats.spanStats.live_bytes || 0,
    livePercentage: details?.data?.results.stats.spanStats.live_percentage || 0,
    sizeInBytes:
      details?.data?.results.stats.spanStats.approximate_disk_bytes || 0,
    rangeCount: details?.data?.results.stats.spanStats.range_count || 0,
    nodesByRegionString: getNodesByRegionString(nodes, nodeRegions, isTenant),
  };
};

// IndexStatsState is defined as shared state between cluster-ui and db-console.
// Currently, this interface mimics cluster-ui's IndexStatsState type, but we
// redefine it here as an interface to decouple.
export interface IndexStatsState {
  data?: TableIndexStatsResponse;
  lastError?: Error;
  valid: boolean;
  inFlight: boolean;
}

export const selectIndexStats = (
  database: string,
  table: string,
  indexUsageStats: Record<string, IndexStatsState>,
): IndexStat[] => {
  const indexStats = indexUsageStats[generateTableID(database, table)];
  const lastReset = TimestampToMoment(indexStats?.data?.last_reset);
  const stats = indexStats?.data?.statistics || [];
  const recommendations = indexStats?.data?.index_recommendations || [];
  const recsMap = buildIndexStatToRecommendationsMap(stats, recommendations);
  return stats.map(indexStat => {
    const indexRecs = recsMap[indexStat?.index_id] || [];
    return indexCombiner(indexStat, lastReset, indexRecs);
  });
};

const indexCombiner = (
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
