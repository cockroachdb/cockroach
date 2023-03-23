// Copyright 2021 The Cockroach Authors.
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
import _ from "lodash";
import {
  DatabaseTablePageData,
  util,
  RecommendationType as RecType,
} from "@cockroachlabs/cluster-ui";

import { cockroach } from "src/js/protos";
import {
  generateTableID,
  refreshTableDetails,
  refreshNodes,
  refreshIndexStats,
  refreshSettings,
  refreshUserSQLRoles,
} from "src/redux/apiReducers";
import { selectHasAdminRole } from "src/redux/user";
import { AdminUIState } from "src/redux/state";
import { databaseNameAttr, tableNameAttr } from "src/util/constants";
import { longToInt } from "src/util/fixLong";
import { getMatchParamByName } from "src/util/query";
import {
  nodeRegionsByIDSelector,
  selectIsMoreThanOneNode,
} from "src/redux/nodes";
import { getNodesByRegionString } from "../utils";
import { resetIndexUsageStatsAction } from "src/redux/indexUsageStats";
import { selectAutomaticStatsCollectionEnabled } from "src/redux/clusterSettings";
import { normalizePrivileges } from "../utils";

const { TableIndexStatsRequest } = cockroach.server.serverpb;

const { RecommendationType } = cockroach.sql.IndexRecommendation;

// Hardcoded isTenant value for db-console.
const isTenant = false;

export const mapStateToProps = createSelector(
  (_state: AdminUIState, props: RouteComponentProps): string =>
    getMatchParamByName(props.match, databaseNameAttr),
  (_state: AdminUIState, props: RouteComponentProps): string =>
    getMatchParamByName(props.match, tableNameAttr),

  state => state.cachedData.tableDetails,
  state => state.cachedData.indexStats,
  state => nodeRegionsByIDSelector(state),
  state => selectIsMoreThanOneNode(state),
  state => selectAutomaticStatsCollectionEnabled(state),
  _ => isTenant,
  state => selectHasAdminRole(state),
  (
    database,
    table,
    tableDetails,
    indexUsageStats,
    nodeRegions,
    showNodeRegionsSection,
    automaticStatsCollectionEnabled,
    isTenant,
    hasAdminRole,
  ): DatabaseTablePageData => {
    const details = tableDetails[generateTableID(database, table)];
    const indexStats = indexUsageStats[generateTableID(database, table)];
    const lastReset = util.TimestampToMoment(indexStats?.data?.last_reset);
    const indexStatsData = _.flatMap(
      indexStats?.data?.statistics,
      indexStat => {
        const lastRead = util.TimestampToMoment(
          indexStat.statistics?.stats?.last_read,
        );
        let lastUsed, lastUsedType;
        if (indexStat.created_at !== null) {
          lastUsed = util.TimestampToMoment(indexStat.created_at);
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

    const userToPrivileges = new Map<string, string[]>();

    details?.data?.results.grantsResp.grants.forEach(grant => {
      if (!userToPrivileges.has(grant.user)) {
        userToPrivileges.set(grant.user, []);
      }
      userToPrivileges.set(
        grant.user,
        userToPrivileges.get(grant.user).concat(grant.privileges),
      );
    });

    const grants = Array.from(userToPrivileges).map(([name, value]) => ({
      user: name,
      privileges: normalizePrivileges(value.sort()),
    }));

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
        indexNames: _.uniq(details?.data?.results.schemaDetails.indexes),
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
      showNodeRegionsSection,
      automaticStatsCollectionEnabled,
      hasAdminRole,
      indexStats: {
        loading: !!indexStats?.inFlight,
        loaded: !!indexStats?.valid,
        lastError: indexStats?.lastError,
        stats: indexStatsData,
        lastReset: lastReset,
      },
    };
  },
);

export const mapDispatchToProps = {
  refreshTableDetails: (database: string, table: string) => {
    return refreshTableDetails({
      database,
      table,
    });
  },
  refreshIndexStats: (database: string, table: string) => {
    return refreshIndexStats(new TableIndexStatsRequest({ database, table }));
  },
  resetIndexUsageStats: resetIndexUsageStatsAction,
  refreshNodes,
  refreshSettings,
  refreshUserSQLRoles,
};
