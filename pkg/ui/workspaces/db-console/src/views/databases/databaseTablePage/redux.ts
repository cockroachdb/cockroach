// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { RouteComponentProps } from "react-router";
import { DatabaseTablePageData, util } from "@cockroachlabs/cluster-ui";

import { cockroach } from "src/js/protos";
import {
  refreshTableDetails,
  refreshNodes,
  refreshIndexStats,
  refreshSettings,
  refreshUserSQLRoles,
} from "src/redux/apiReducers";
import { resetIndexUsageStatsAction } from "src/redux/indexUsageStats";
import { AdminUIState } from "src/redux/state";
import {
  nodeRegionsByIDSelector,
  selectIsMoreThanOneNode,
} from "src/redux/nodes";
import {
  deriveIndexDetailsMemoized,
  deriveTablePageDetailsMemoized,
} from "@cockroachlabs/cluster-ui";
import { selectHasAdminRole } from "src/redux/user";
import {
  selectAutomaticStatsCollectionEnabled,
  selectDropUnusedIndexDuration,
  selectIndexRecommendationsEnabled,
  selectIndexUsageStatsEnabled,
} from "src/redux/clusterSettings";
import { getMatchParamByName } from "src/util/query";
import { databaseNameAttr, tableNameAttr } from "src/util/constants";

const { TableIndexStatsRequest } = cockroach.server.serverpb;

// Hardcoded isTenant value for db-console.
const isTenant = false;

export const mapStateToProps = (
  state: AdminUIState,
  props: RouteComponentProps,
): DatabaseTablePageData => {
  const database = getMatchParamByName(props.match, databaseNameAttr);
  const table = getMatchParamByName(props.match, tableNameAttr);
  const tableDetails = state?.cachedData.tableDetails;
  const details = tableDetails[util.generateTableID(database, table)];
  const indexUsageStats = state?.cachedData.indexStats;
  const indexStats = indexUsageStats[util.generateTableID(database, table)];
  const lastReset = util.TimestampToMoment(
    indexStats?.data?.last_reset,
    util.minDate,
  );
  const nodeRegions = nodeRegionsByIDSelector(state);
  const nodeStatuses = state?.cachedData.nodes.data;

  return {
    databaseName: database,
    name: table,
    schemaName: "",
    details: deriveTablePageDetailsMemoized({
      details,
      nodeRegions,
      isTenant,
      nodeStatuses,
    }),
    showNodeRegionsSection: selectIsMoreThanOneNode(state) && !isTenant,
    automaticStatsCollectionEnabled:
      selectAutomaticStatsCollectionEnabled(state) || false,
    hasAdminRole: selectHasAdminRole(state) || false,
    showIndexRecommendations: selectIndexRecommendationsEnabled(state),
    csIndexUnusedDuration: selectDropUnusedIndexDuration(state),
    indexUsageStatsEnabled: selectIndexUsageStatsEnabled(state),
    indexStats: {
      loading: !!indexStats?.inFlight,
      loaded: !!indexStats?.valid,
      lastError: indexStats?.lastError,
      stats: deriveIndexDetailsMemoized({ database, table, indexUsageStats }),
      lastReset: lastReset,
    },
    isTenant,
  };
};

export const mapDispatchToProps = {
  refreshTableDetails: (
    database: string,
    table: string,
    csIndexUnusedDuration: string,
  ) => {
    return refreshTableDetails({
      database,
      table,
      csIndexUnusedDuration,
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
