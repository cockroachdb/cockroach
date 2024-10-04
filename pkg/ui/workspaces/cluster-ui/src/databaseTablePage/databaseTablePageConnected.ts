// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { RouteComponentProps } from "react-router";
import { AppState, uiConfigActions } from "src/store";
import {
  databaseNameCCAttr,
  generateTableID,
  getMatchParamByName,
  minDate,
  schemaNameAttr,
  tableNameCCAttr,
  TimestampToMoment,
} from "src/util";
import {
  actions as nodesActions,
  nodeRegionsByIDSelector,
} from "src/store/nodes";
import {
  selectAutomaticStatsCollectionEnabled,
  selectDropUnusedIndexDuration,
  selectIndexRecommendationsEnabled,
  selectIndexUsageStatsEnabled,
} from "src/store/clusterSettings/clusterSettings.selectors";
import { selectHasAdminRole, selectIsTenant } from "src/store/uiConfig";
import {
  DatabaseTablePageActions,
  DatabaseTablePageData,
  DatabaseTablePage,
} from "./databaseTablePage";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { Dispatch } from "redux";
import { actions as tableDetailsActions } from "src/store/databaseTableDetails";
import { actions as indexStatsActions } from "src/store/indexStats";
import { actions as analyticsActions } from "src/store/analytics";
import { actions as clusterSettingsActions } from "src/store/clusterSettings";
import {
  deriveIndexDetailsMemoized,
  deriveTablePageDetailsMemoized,
} from "../databases";
import { withRouter } from "react-router-dom";
import { connect } from "react-redux";

export const mapStateToProps = (
  state: AppState,
  props: RouteComponentProps,
): DatabaseTablePageData => {
  const database = getMatchParamByName(props.match, databaseNameCCAttr);
  const table = getMatchParamByName(props.match, tableNameCCAttr);
  const schema = getMatchParamByName(props.match, schemaNameAttr);
  const tableDetails = state.adminUI?.tableDetails;
  const indexUsageStats = state.adminUI?.indexStats;
  const details = tableDetails[generateTableID(database, table)];
  const indexStatsState = indexUsageStats[generateTableID(database, table)];
  const lastReset = TimestampToMoment(
    indexStatsState?.data?.last_reset,
    minDate,
  );
  const nodeRegions = nodeRegionsByIDSelector(state);
  const isTenant = selectIsTenant(state);
  const nodeStatuses = state.adminUI?.nodes.data;
  return {
    databaseName: database,
    name: table,
    schemaName: schema,
    details: deriveTablePageDetailsMemoized({
      details,
      nodeRegions,
      isTenant,
      nodeStatuses,
    }),
    showNodeRegionsSection: Object.keys(nodeRegions).length > 1 && !isTenant,
    automaticStatsCollectionEnabled:
      selectAutomaticStatsCollectionEnabled(state),
    indexUsageStatsEnabled: selectIndexUsageStatsEnabled(state),
    showIndexRecommendations: selectIndexRecommendationsEnabled(state),
    csIndexUnusedDuration: selectDropUnusedIndexDuration(state),
    hasAdminRole: selectHasAdminRole(state),
    indexStats: {
      loading: !!indexStatsState?.inFlight,
      loaded: !!indexStatsState?.valid,
      lastError: indexStatsState?.lastError,
      stats: deriveIndexDetailsMemoized({ database, table, indexUsageStats }),
      lastReset,
    },
  };
};

export const mapDispatchToProps = (
  dispatch: Dispatch,
): DatabaseTablePageActions => ({
  refreshTableDetails: (
    database: string,
    table: string,
    csIndexUnusedDuration: string,
  ) => {
    dispatch(
      tableDetailsActions.refresh({ database, table, csIndexUnusedDuration }),
    );
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
    dispatch(nodesActions.refresh());
  },
  refreshSettings: () => {
    dispatch(clusterSettingsActions.refresh());
  },
  refreshUserSQLRoles: () => dispatch(uiConfigActions.refreshUserSQLRoles()),
});

export const ConnectedDatabaseTablePage = withRouter(
  connect<DatabaseTablePageData, DatabaseTablePageActions, RouteComponentProps>(
    mapStateToProps,
    mapDispatchToProps,
  )(DatabaseTablePage),
);
