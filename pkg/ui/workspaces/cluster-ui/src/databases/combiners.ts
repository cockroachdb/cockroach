// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { createSelector } from "@reduxjs/toolkit";

import { DatabaseDetailsPageDataTable } from "src/databaseDetailsPage";

import { DatabasesListResponse, TableNameParts } from "../api";
import { DatabasesPageDataDatabase } from "../databasesPage";
import { DatabaseTablePageDataDetails, IndexStat } from "../databaseTablePage";
import { RecommendationType as RecType } from "../indexDetailsPage";
import {
  DatabaseDetailsSpanStatsState,
  DatabaseDetailsState,
} from "../store/databaseDetails";
import { TableDetailsState } from "../store/databaseTableDetails";
import { IndexStatsState } from "../store/indexStats";
import { generateTableID, longToInt, TimestampToMoment } from "../util";

import {
  Nodes,
  Stores,
  buildIndexStatToRecommendationsMap,
  getNodeIdsFromStoreIds,
  getNodesByRegionString,
  normalizePrivileges,
  normalizeRoles,
} from "./util";
type IndexUsageStatistic =
  cockroach.server.serverpb.TableIndexStatsResponse.IExtendedCollectedIndexUsageStatistics;
const { RecommendationType } = cockroach.sql.IndexRecommendation;

interface DerivedDatabaseDetailsParams {
  dbListResp: DatabasesListResponse;
  databaseDetails: Record<string, DatabaseDetailsState>;
  spanStats: Record<string, DatabaseDetailsSpanStatsState>;
  nodeRegions: Record<string, string>;
  isTenant: boolean;
  /** A list of node statuses so that store ids can be mapped to nodes. */
  nodeStatuses: cockroach.server.status.statuspb.INodeStatus[];
}

export const deriveDatabaseDetailsMemoized = createSelector(
  (params: DerivedDatabaseDetailsParams) => params.dbListResp,
  (params: DerivedDatabaseDetailsParams) => params.databaseDetails,
  (params: DerivedDatabaseDetailsParams) => params.spanStats,
  (params: DerivedDatabaseDetailsParams) => params.nodeRegions,
  (params: DerivedDatabaseDetailsParams) => params.isTenant,
  (params: DerivedDatabaseDetailsParams) => params.nodeStatuses,
  (
    dbListResp,
    databaseDetails,
    spanStats,
    nodeRegions,
    isTenant,
    nodeStatuses,
  ): DatabasesPageDataDatabase[] => {
    const databases = dbListResp?.databases ?? [];
    return databases.map(dbName => {
      const dbDetails = databaseDetails[dbName];
      const spanStatsForDB = spanStats[dbName];
      return deriveDatabaseDetails(
        dbName,
        dbDetails,
        spanStatsForDB,
        nodeRegions,
        isTenant,
        nodeStatuses,
      );
    });
  },
);

const deriveDatabaseDetails = (
  database: string,
  dbDetails: DatabaseDetailsState,
  spanStats: DatabaseDetailsSpanStatsState,
  nodeRegionsByID: Record<string, string>,
  isTenant: boolean,
  nodeStatuses: cockroach.server.status.statuspb.INodeStatus[],
): DatabasesPageDataDatabase => {
  const dbStats = dbDetails?.data?.results.stats;
  /** List of store IDs for the current cluster. All of the values in the
   * `*replicas` columns correspond to store IDs. */
  const stores: Stores = {
    kind: "store",
    ids: dbStats?.replicaData.storeIDs || [],
  };
  /** List of node IDs for the current cluster. */
  const nodes = getNodeIdsFromStoreIds(stores, nodeStatuses);

  const nodesByRegionString = getNodesByRegionString(
    nodes,
    nodeRegionsByID,
    isTenant,
  );
  const numIndexRecommendations =
    dbStats?.indexStats.num_index_recommendations || 0;

  return {
    detailsLoading: !!dbDetails?.inFlight,
    detailsLoaded: !!dbDetails?.valid,
    spanStatsLoading: !!spanStats?.inFlight,
    spanStatsLoaded: !!spanStats?.valid,
    detailsRequestError: dbDetails?.lastError, // http request error.
    spanStatsRequestError: spanStats?.lastError, // http request error.
    detailsQueryError: dbDetails?.data?.results?.error,
    spanStatsQueryError: spanStats?.data?.results?.error,
    name: database,
    spanStats: spanStats?.data?.results.spanStats,
    tables: dbDetails?.data?.results.tablesResp,
    nodes: nodes.ids,
    nodesByRegionString,
    numIndexRecommendations,
  };
};

interface DerivedTableDetailsParams {
  dbName: string;
  tables: TableNameParts[];
  tableDetails: Record<string, TableDetailsState>;
  nodeRegions: Record<string, string>;
  isTenant: boolean;
  /** A list of node statuses so that store ids can be mapped to nodes. */
  nodeStatuses: cockroach.server.status.statuspb.INodeStatus[];
}

export const deriveTableDetailsMemoized = createSelector(
  (params: DerivedTableDetailsParams) => params.dbName,
  (params: DerivedTableDetailsParams) => params.tables,
  (params: DerivedTableDetailsParams) => params.tableDetails,
  (params: DerivedTableDetailsParams) => params.nodeRegions,
  (params: DerivedTableDetailsParams) => params.isTenant,
  (params: DerivedTableDetailsParams) => params.nodeStatuses,
  (
    dbName,
    tables,
    tableDetails,
    nodeRegions,
    isTenant,
    nodeStatuses,
  ): DatabaseDetailsPageDataTable[] => {
    tables = tables || [];
    return tables.map(table => {
      const tableID = generateTableID(
        dbName,
        table.qualifiedNameWithSchemaAndTable,
      );
      const details = tableDetails[tableID];
      return deriveDatabaseTableDetails(
        table,
        details,
        nodeRegions,
        isTenant,
        nodeStatuses,
      );
    });
  },
);

const deriveDatabaseTableDetails = (
  table: TableNameParts,
  details: TableDetailsState,
  nodeRegions: Record<string, string>,
  isTenant: boolean,
  nodeStatuses: cockroach.server.status.statuspb.INodeStatus[],
): DatabaseDetailsPageDataTable => {
  const results = details?.data?.results;
  const grants = results?.grantsResp.grants ?? [];
  const normalizedRoles = normalizeRoles(grants.map(grant => grant.user));
  const normalizedPrivileges = normalizePrivileges(
    [].concat(...grants.map(grant => grant.privileges)),
  );
  const stores: Stores = {
    kind: "store",
    ids: results?.stats.replicaData.storeIDs || [],
  };
  const nodes: Nodes = getNodeIdsFromStoreIds(stores, nodeStatuses);
  return {
    name: table,
    qualifiedDisplayName: `${table.schema}.${table.table}`,
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
      nodes: nodes.ids,
      nodesByRegionString: getNodesByRegionString(nodes, nodeRegions, isTenant),
    },
  };
};

interface DerivedTablePageDetailsParams {
  details: TableDetailsState;
  nodeRegions: Record<string, string>;
  isTenant: boolean;
  /** A list of node statuses so that store ids can be mapped to nodes. */
  nodeStatuses: cockroach.server.status.statuspb.INodeStatus[];
}

export const deriveTablePageDetailsMemoized = createSelector(
  (params: DerivedTablePageDetailsParams) => params.details,
  (params: DerivedTablePageDetailsParams) => params.nodeRegions,
  (params: DerivedTablePageDetailsParams) => params.isTenant,
  (params: DerivedTablePageDetailsParams) => params.nodeStatuses,
  (
    details,
    nodeRegions,
    isTenant,
    nodeStatuses,
  ): DatabaseTablePageDataDetails => {
    const results = details?.data?.results;
    const grants = results?.grantsResp.grants || [];
    const normalizedGrants =
      grants.map(grant => ({
        user: grant.user,
        privileges: normalizePrivileges(grant.privileges),
      })) || [];

    const stores: Stores = {
      kind: "store",
      ids: results?.stats.replicaData.storeIDs || [],
    };
    const nodes = getNodeIdsFromStoreIds(stores, nodeStatuses);

    return {
      loading: !!details?.inFlight,
      loaded: !!details?.valid,
      requestError: details?.lastError,
      queryError: results?.error,
      createStatement: results?.createStmtResp,
      replicaData: results?.stats?.replicaData,
      indexData: results?.schemaDetails,
      grants: { all: normalizedGrants, error: results?.grantsResp?.error },
      statsLastUpdated: results?.heuristicsDetails,
      spanStats: results?.stats?.spanStats,
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
