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
  selectIndexStats,
  selectTablePageDataDetails,
} from "@cockroachlabs/cluster-ui";
import { selectHasAdminRole } from "src/redux/user";
import { selectAutomaticStatsCollectionEnabled } from "src/redux/clusterSettings";
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
  const lastReset = indexStats?.data?.last_reset
    ? util.TimestampToMoment(indexStats?.data?.last_reset)
    : null;
  const nodeRegions = nodeRegionsByIDSelector(state);

  return {
    databaseName: database,
    name: table,
    details: selectTablePageDataDetails(details, nodeRegions, isTenant),
    showNodeRegionsSection: selectIsMoreThanOneNode(state),
    automaticStatsCollectionEnabled:
      selectAutomaticStatsCollectionEnabled(state),
    hasAdminRole: selectHasAdminRole(state),
    indexStats: {
      loading: !!indexStats?.inFlight,
      loaded: !!indexStats?.valid,
      lastError: indexStats?.lastError,
      stats: selectIndexStats(database, table, indexUsageStats),
      lastReset: lastReset,
    },
  };
};

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
