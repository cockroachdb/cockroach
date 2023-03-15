// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { RouteComponentProps } from "react-router";
import { createSelector } from "reselect";
import {AppState, uiConfigActions} from "../store";
import {
  databaseNameAttr,
  generateTableID,
  getMatchParamByName,
  getNodesByRegionString,
  longToInt, normalizePrivileges,
  tableNameAttr,
  TimestampToMoment
} from "../util";
import {selectTableDetails} from "../store/databaseTableDetails/tableDetails.selector";
import {actions as nodesActions, nodeRegionsByIDSelector} from "../store/nodes";
import {selectAutomaticStatsCollectionEnabled} from "../store/clusterSettings/clusterSettings.selectors";
import {selectHasAdminRole, selectIsTenant} from "../store/uiConfig";
import {DatabaseTablePageActions, DatabaseTablePageData, IndexStat} from "./databaseTablePage";
import { RecommendationType as RecType } from "../indexDetailsPage";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import {Dispatch} from "redux";
import {actions as tableDetailsActions} from "../store/databaseTableDetails";
import {actions as indexStatsActions} from "../store/indexStats";
import {actions as analyticsActions} from "../store/analytics";
import {actions as clusterSettingsActions} from "../store/clusterSettings";
const { RecommendationType } = cockroach.sql.IndexRecommendation;

const selectIndexStats = (state: AppState, props: RouteComponentProps): IndexStat[] => {
  const database = getMatchParamByName(props.match, databaseNameAttr);
  const table = getMatchParamByName(props.match, tableNameAttr);
  const indexUsageStats = state.adminUI?.indexStats.cachedData;
  const indexStats = indexUsageStats[generateTableID(database, table)];
  const lastReset = TimestampToMoment(indexStats?.data?.last_reset);
  return indexStats?.data?.statistics.map(indexStat => {
      const lastRead = TimestampToMoment(
        indexStat.statistics?.stats?.last_read,
      );
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
      const filteredIndexRecommendations =
        indexStats?.data?.index_recommendations.filter(
          indexRec =>
            indexRec.index_id === indexStat?.statistics.key.index_id,
        ) || [];
      const indexRecommendations = filteredIndexRecommendations.map(
        indexRec => {
          let type: RecType = "Unknown";
          switch (RecommendationType[indexRec.type].toString()) {
            case "DROP_UNUSED":
              type = "DROP_UNUSED";
          }
          return {
            type: type,
            reason: indexRec.reason,
          };
        },
      );
      return {
        indexName: indexStat.index_name,
        totalReads: longToInt(indexStat.statistics?.stats?.total_read_count),
        lastUsed: lastUsed,
        lastUsedType: lastUsedType,
        indexRecommendations,
      };
    },
  );
}

export const mapStateToProps = createSelector(
  (_state: AppState, props: RouteComponentProps): string =>
    getMatchParamByName(props.match, databaseNameAttr),
  (_state: AppState, props: RouteComponentProps): string =>
    getMatchParamByName(props.match, tableNameAttr),

  state => selectTableDetails(state),
  state => state.adminUI?.indexStats.cachedData,
  state => nodeRegionsByIDSelector(state),
  state => selectAutomaticStatsCollectionEnabled(state),
  state => selectIsTenant(state),
  state => selectHasAdminRole(state),
  (state: AppState, props: RouteComponentProps) => selectIndexStats(state, props),
  (
    database,
    table,
    tableDetails,
    indexUsageStats,
    nodeRegions,
    automaticStatsCollectionEnabled,
    isTenant,
    hasAdminRole,
    indexStats,
  ): DatabaseTablePageData => {
    const details = tableDetails.cache[generateTableID(database, table)];
    const indexStatsState = indexUsageStats[generateTableID(database, table)];
    const lastReset = TimestampToMoment(indexStatsState?.data?.last_reset);
    const grants = details?.data?.results.grantsResp.grants.map(grant => ({
      user: grant.user,
      privileges: normalizePrivileges(grant.privileges),
    })) || [];

    const nodes = details?.data?.results.stats.replicaData.nodeIDs || [];

    return {
      databaseName: database,
      name: table,
      details: {
        loading: !!details?.inFlight,
        loaded: !!details?.valid,
        lastError: details?.lastError,
        createStatement:
          details?.data?.results.createStmtResp.create_statement || "",
        replicaCount:
          details?.data?.results.stats.replicaData.replicaCount || 0,
        indexNames: details?.data?.results.schemaDetails.indexes,
        grants: grants,
        statsLastUpdated:
          details?.data?.results.heuristicsDetails.stats_last_created_at ||
          null,
        totalBytes: details?.data?.results.stats.spanStats.total_bytes || 0,
        liveBytes: details?.data?.results.stats.spanStats.live_bytes || 0,
        livePercentage:
          details?.data?.results.stats.spanStats.live_percentage || 0,
        sizeInBytes:
          details?.data?.results.stats.spanStats.approximate_disk_bytes || 0,
        rangeCount: details?.data?.results.stats.spanStats.range_count || 0,
        nodesByRegionString: getNodesByRegionString(
          nodes,
          nodeRegions,
          isTenant,
        ),
      },
      // Default setting this to true. On db-console we only display this when
      // we have >1 node, but I'm not sure why.
      showNodeRegionsSection: true,
      automaticStatsCollectionEnabled,
      hasAdminRole,
      indexStats: {
        loading: !!indexStatsState?.inFlight,
        loaded: !!indexStatsState?.valid,
        lastError: indexStatsState?.lastError,
        stats: indexStats,
        lastReset,
      },
    };
  },
);

export const mapDispatchToProps = (dispatch: Dispatch): DatabaseTablePageActions => ({
  refreshTableDetails: (database: string, table: string) => {
    dispatch(tableDetailsActions.refresh({database, table}))
  },
  refreshIndexStats: (database: string, table: string) => {
    dispatch(
      indexStatsActions.refresh(
        new cockroach.server.serverpb.TableIndexStatsRequest({
          database,
          table,
        }),
      ),
    );
  },
  resetIndexUsageStats: (database: string, table: string) => {
    dispatch(
      indexStatsActions.reset({
        database,
        table,
      }),
    );
    dispatch(
      analyticsActions.track({
        name: "Reset Index Usage",
        page: "Index Details",
      }),
    );
  },
  refreshNodes: () => {
    dispatch(nodesActions.refresh())
  },
  refreshSettings: () => {
    dispatch(clusterSettingsActions.refresh())
  },
  refreshUserSQLRoles: () => dispatch(uiConfigActions.refreshUserSQLRoles()),
});
