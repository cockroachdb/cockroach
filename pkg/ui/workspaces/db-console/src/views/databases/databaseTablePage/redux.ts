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
import { DatabaseTablePageData, util } from "@cockroachlabs/cluster-ui";

import { cockroach } from "src/js/protos";
import {
  generateTableID,
  refreshTableDetails,
  refreshTableStats,
  refreshNodes,
  refreshIndexStats,
  refreshSettings,
} from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { databaseNameAttr, tableNameAttr } from "src/util/constants";
import { FixLong, longToInt } from "src/util/fixLong";
import { getMatchParamByName } from "src/util/query";
import {
  nodeRegionsByIDSelector,
  selectIsMoreThanOneNode,
} from "src/redux/nodes";
import { getNodesByRegionString } from "../utils";
import { resetIndexUsageStatsAction } from "src/redux/indexUsageStats";
import { selectAutomaticStatsCollectionEnabled } from "src/redux/clusterSettings";

const {
  TableDetailsRequest,
  TableStatsRequest,
  TableIndexStatsRequest,
} = cockroach.server.serverpb;

export const mapStateToProps = createSelector(
  (_state: AdminUIState, props: RouteComponentProps): string =>
    getMatchParamByName(props.match, databaseNameAttr),
  (_state: AdminUIState, props: RouteComponentProps): string =>
    getMatchParamByName(props.match, tableNameAttr),

  state => state.cachedData.tableDetails,
  state => state.cachedData.tableStats,
  state => state.cachedData.indexStats,
  state => nodeRegionsByIDSelector(state),
  state => selectIsMoreThanOneNode(state),
  state => selectAutomaticStatsCollectionEnabled(state),

  (
    database,
    table,
    tableDetails,
    tableStats,
    indexUsageStats,
    nodeRegions,
    showNodeRegionsSection,
    automaticStatsCollectionEnabled,
  ): DatabaseTablePageData => {
    const details = tableDetails[generateTableID(database, table)];
    const stats = tableStats[generateTableID(database, table)];
    const indexStats = indexUsageStats[generateTableID(database, table)];
    const lastReset = util.TimestampToMoment(indexStats?.data?.last_reset);
    const indexStatsData = _.flatMap(
      indexStats?.data?.statistics,
      indexStat => {
        const lastRead = util.TimestampToMoment(
          indexStat.statistics?.stats?.last_read,
        );
        let lastUsed, lastUsedType;
        if (lastRead.isAfter(lastReset)) {
          lastUsed = lastRead;
          lastUsedType = "read";
        } else {
          lastUsed = lastReset;
          lastUsedType = "reset";
        }
        return {
          indexName: indexStat.index_name,
          totalReads: longToInt(indexStat.statistics?.stats?.total_read_count),
          lastUsed: lastUsed,
          lastUsedType: lastUsedType,
        };
      },
    );
    const grants = _.flatMap(details?.data?.grants, grant =>
      _.map(grant.privileges, privilege => {
        return { user: grant.user, privilege };
      }),
    );
    const nodes = stats?.data?.node_ids || [];

    return {
      databaseName: database,
      name: table,
      details: {
        loading: !!details?.inFlight,
        loaded: !!details?.valid,
        createStatement: details?.data?.create_table_statement || "",
        replicaCount: details?.data?.zone_config?.num_replicas || 0,
        indexNames: _.uniq(_.map(details?.data?.indexes, index => index.name)),
        grants: grants,
        statsLastUpdated: details?.data?.stats_last_created_at
          ? util.TimestampToMoment(details?.data?.stats_last_created_at)
          : null,
      },
      showNodeRegionsSection,
      automaticStatsCollectionEnabled,
      stats: {
        loading: !!stats?.inFlight,
        loaded: !!stats?.valid,
        sizeInBytes: FixLong(
          stats?.data?.approximate_disk_bytes || 0,
        ).toNumber(),
        rangeCount: FixLong(stats?.data?.range_count || 0).toNumber(),
        nodesByRegionString: getNodesByRegionString(nodes, nodeRegions),
      },
      indexStats: {
        loading: !!indexStats?.inFlight,
        loaded: !!indexStats?.valid,
        stats: indexStatsData,
        lastReset: lastReset,
      },
    };
  },
);

export const mapDispatchToProps = {
  refreshTableDetails: (database: string, table: string) => {
    return refreshTableDetails(new TableDetailsRequest({ database, table }));
  },

  refreshTableStats: (database: string, table: string) => {
    return refreshTableStats(new TableStatsRequest({ database, table }));
  },

  refreshIndexStats: (database: string, table: string) => {
    return refreshIndexStats(new TableIndexStatsRequest({ database, table }));
  },

  resetIndexUsageStats: resetIndexUsageStatsAction,

  refreshNodes,

  refreshSettings,
};
