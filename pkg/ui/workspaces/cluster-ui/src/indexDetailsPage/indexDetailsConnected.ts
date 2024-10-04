// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { AppState, uiConfigActions } from "../store";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { Dispatch } from "redux";
import {
  IndexDetailPageActions,
  IndexDetailsPage,
  IndexDetailsPageData,
  RecommendationType as RecType,
} from "./indexDetailsPage";
import { connect } from "react-redux";
import { actions as indexStatsActions } from "src/store/indexStats/indexStats.reducer";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import {
  actions as nodesActions,
  nodeRegionsByIDSelector,
} from "../store/nodes";
import { TimeScale } from "../timeScaleDropdown";
import { actions as sqlStatsActions } from "../store/sqlStats";
import { actions as analyticsActions } from "../store/analytics";
import {
  databaseNameAttr,
  generateTableID,
  getMatchParamByName,
  indexNameAttr,
  longToInt,
  schemaNameAttr,
  tableNameAttr,
  TimestampToMoment,
} from "../util";
import { BreadcrumbItem } from "../breadcrumbs";
import {
  selectHasAdminRole,
  selectHasViewActivityRedactedRole,
  selectIsTenant,
} from "../store/uiConfig";
import { selectTimeScale } from "../store/utils/selectors";
import RecommendationType = cockroach.sql.IndexRecommendation.RecommendationType;

// Note: if the managed-service routes to the index detail or the previous
// database pages change, the breadcrumbs displayed here need to be updated.
// TODO(thomas): ensure callers are splitting schema/table name correctly
function createManagedServiceBreadcrumbs(
  database: string,
  schema: string,
  table: string,
  index: string,
): BreadcrumbItem[] {
  return [
    { link: "/databases", name: "Databases" },
    {
      link: `/databases/${database}`,
      name: "Tables",
    },
    {
      link: `/databases/${database}/${schema}/${table}`,
      name: `Table: ${table}`,
    },
    {
      link: `/databases/${database}/${schema}/${table}/${index}`,
      name: `Index: ${index}`,
    },
  ];
}

const mapStateToProps = (
  state: AppState,
  props: RouteComponentProps,
): IndexDetailsPageData => {
  const databaseName = getMatchParamByName(props.match, databaseNameAttr);
  const schemaName = getMatchParamByName(props.match, schemaNameAttr);
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
    breadcrumbItems: createManagedServiceBreadcrumbs(
      databaseName,
      schemaName,
      tableName,
      indexName,
    ),
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

export const ConnectedIndexDetailsPage = withRouter<any, any>(
  connect(mapStateToProps, mapDispatchToProps)(IndexDetailsPage),
);
