// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  IndexDetailsPageData,
  util,
  RecommendationType as RecType,
} from "@cockroachlabs/cluster-ui";
import { RouteComponentProps } from "react-router";

import { cockroach } from "src/js/protos";
import {
  refreshIndexStats,
  refreshNodes,
  refreshUserSQLRoles,
} from "src/redux/apiReducers";
import { resetIndexUsageStatsAction } from "src/redux/indexUsageStats";
import { nodeRegionsByIDSelector } from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import { setGlobalTimeScaleAction } from "src/redux/statements";
import { selectTimeScale } from "src/redux/timeScale";
import {
  selectHasViewActivityRedactedRole,
  selectHasAdminRole,
} from "src/redux/user";
import {
  databaseNameAttr,
  tableNameAttr,
  indexNameAttr,
} from "src/util/constants";
import { longToInt } from "src/util/fixLong";
import { getMatchParamByName } from "src/util/query";

import TableIndexStatsRequest = cockroach.server.serverpb.TableIndexStatsRequest;
const { RecommendationType } = cockroach.sql.IndexRecommendation;

export const mapStateToProps = (
  state: AdminUIState,
  props: RouteComponentProps,
): IndexDetailsPageData => {
  const database = getMatchParamByName(props.match, databaseNameAttr);
  const table = getMatchParamByName(props.match, tableNameAttr);
  const index = getMatchParamByName(props.match, indexNameAttr);
  const indexStats = state.cachedData.indexStats;
  const hasViewActivityRedactedRole = selectHasViewActivityRedactedRole(state);
  const nodeRegions = nodeRegionsByIDSelector(state);
  const hasAdminRole = selectHasAdminRole(state);
  const timeScale = selectTimeScale(state);
  const stats = indexStats[util.generateTableID(database, table)];
  const details = stats?.data?.statistics.filter(
    stat => stat.index_name === index, // index names must be unique for a table
  )[0];
  const filteredIndexRecommendations =
    stats?.data?.index_recommendations.filter(
      indexRec => indexRec.index_id === details?.statistics.key.index_id,
    ) || [];
  const indexRecommendations = filteredIndexRecommendations.map(indexRec => {
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
    databaseName: database,
    tableName: table,
    indexName: index,
    isTenant: false,
    hasViewActivityRedactedRole: hasViewActivityRedactedRole,
    hasAdminRole: hasAdminRole,
    nodeRegions: nodeRegions,
    timeScale: timeScale,
    details: {
      loading: !!stats?.inFlight,
      loaded: !!stats?.valid,
      createStatement: details?.create_statement || "",
      tableID: details?.statistics.key.table_id.toString(),
      indexID: details?.statistics.key.index_id.toString(),
      totalReads: longToInt(details?.statistics?.stats?.total_read_count) || 0,
      lastRead: util.TimestampToMoment(
        details?.statistics?.stats?.last_read,
        util.minDate,
      ),
      lastReset: util.TimestampToMoment(stats?.data?.last_reset, util.minDate),
      indexRecommendations,
      databaseID: stats?.data?.database_id,
    },
  };
};

export const mapDispatchToProps = {
  refreshIndexStats: (database: string, table: string) => {
    return refreshIndexStats(new TableIndexStatsRequest({ database, table }));
  },
  resetIndexUsageStats: resetIndexUsageStatsAction,
  refreshNodes,
  refreshUserSQLRoles,
  onTimeScaleChange: setGlobalTimeScaleAction,
};
