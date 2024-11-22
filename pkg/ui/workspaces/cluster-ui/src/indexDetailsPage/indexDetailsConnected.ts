// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { Dispatch } from "redux";

import { actions as indexStatsActions } from "src/store/indexStats/indexStats.reducer";

import { AppState, uiConfigActions } from "../store";
import { actions as analyticsActions } from "../store/analytics";
import {
  actions as nodesActions,
  nodeRegionsByIDSelector,
} from "../store/nodes";
import { actions as sqlStatsActions } from "../store/sqlStats";
import {
  selectHasAdminRole,
  selectHasViewActivityRedactedRole,
  selectIsTenant,
} from "../store/uiConfig";
import { selectTimeScale } from "../store/utils/selectors";
import { TimeScale } from "../timeScaleDropdown";
import {
  databaseNameAttr,
  generateTableID,
  getMatchParamByName,
  indexNameAttr,
  longToInt,
  tableNameAttr,
  TimestampToMoment,
} from "../util";

import {
  IndexDetailPageActions,
  IndexDetailsPage,
  IndexDetailsPageData,
  RecommendationType as RecType,
} from "./indexDetailsPage";

import RecommendationType = cockroach.sql.IndexRecommendation.RecommendationType;

const mapStateToProps = (
  state: AppState,
  props: RouteComponentProps,
): IndexDetailsPageData => {
  const databaseName = getMatchParamByName(props.match, databaseNameAttr);
  const tableName = getMatchParamByName(props.match, tableNameAttr);
  const indexName = getMatchParamByName(props.match, indexNameAttr);

  const stats =
    state.adminUI?.indexStats[generateTableID(databaseName, tableName)];
  const details = stats?.data?.statistics.find(
    stat => stat.index_name === indexName, // index names must be unique for a table
  );
  const filteredIndexRecommendations =
    stats?.data?.index_recommendations.filter(
      indexRec => indexRec.index_id === details?.statistics.key.index_id,
    ) || [];
  const indexRecommendations = filteredIndexRecommendations.map(indexRec => ({
    type: (RecommendationType[indexRec.type]?.toString() ||
      "Unknown") as RecType,
    reason: indexRec.reason,
  }));
  return {
    databaseName,
    hasAdminRole: selectHasAdminRole(state),
    hasViewActivityRedactedRole: selectHasViewActivityRedactedRole(state),
    indexName,
    isTenant: selectIsTenant(state),
    nodeRegions: nodeRegionsByIDSelector(state),
    tableName,
    timeScale: selectTimeScale(state),
    details: {
      loading: !!stats?.inFlight,
      loaded: !!stats?.valid,
      createStatement: details?.create_statement || "",
      tableID: details?.statistics.key.table_id.toString(),
      indexID: details?.statistics.key.index_id.toString(),
      totalReads: longToInt(details?.statistics?.stats?.total_read_count) || 0,
      lastRead: TimestampToMoment(details?.statistics?.stats?.last_read),
      lastReset: TimestampToMoment(stats?.data?.last_reset),
      indexRecommendations,
      databaseID: stats?.data?.database_id,
    },
  };
};

const mapDispatchToProps = (dispatch: Dispatch): IndexDetailPageActions => ({
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
  refreshNodes: () => dispatch(nodesActions.refresh()),
  refreshUserSQLRoles: () => dispatch(uiConfigActions.refreshUserSQLRoles()),
  onTimeScaleChange: (ts: TimeScale) => {
    dispatch(
      sqlStatsActions.updateTimeScale({
        ts: ts,
      }),
    );
    dispatch(
      analyticsActions.track({
        name: "TimeScale changed",
        page: "Index Details",
        value: ts.key,
      }),
    );
  },
});

export const ConnectedIndexDetailsPage = withRouter(
  connect(mapStateToProps, mapDispatchToProps)(IndexDetailsPage),
);
