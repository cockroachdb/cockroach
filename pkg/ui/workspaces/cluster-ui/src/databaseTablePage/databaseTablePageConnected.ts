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
import { AppState, uiConfigActions } from "../store";
import {
  databaseNameAttr,
  generateTableID,
  getMatchParamByName,
  minDate,
  tableNameAttr,
  TimestampToMoment,
} from "../util";
import {
  actions as nodesActions,
  nodeRegionsByIDSelector,
} from "../store/nodes";
import { selectAutomaticStatsCollectionEnabled } from "../store/clusterSettings/clusterSettings.selectors";
import { selectHasAdminRole, selectIsTenant } from "../store/uiConfig";
import {
  DatabaseTablePageActions,
  DatabaseTablePageData,
} from "./databaseTablePage";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { Dispatch } from "redux";
import { actions as tableDetailsActions } from "../store/databaseTableDetails";
import { actions as indexStatsActions } from "../store/indexStats";
import { actions as analyticsActions } from "../store/analytics";
import { actions as clusterSettingsActions } from "../store/clusterSettings";
import { selectIndexStats, selectTablePageDataDetails } from "../selectors";

export const mapStateToProps = (
  state: AppState,
  props: RouteComponentProps,
): DatabaseTablePageData => {
  const database = getMatchParamByName(props.match, databaseNameAttr);
  const table = getMatchParamByName(props.match, tableNameAttr);
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

  return {
    databaseName: database,
    name: table,
    details: selectTablePageDataDetails(details, nodeRegions, isTenant),
    // Default setting this to true. On db-console we only display this when
    // we have >1 node, but I'm not sure why.
    showNodeRegionsSection: true,
    automaticStatsCollectionEnabled:
      selectAutomaticStatsCollectionEnabled(state),
    hasAdminRole: selectHasAdminRole(state),
    indexStats: {
      loading: !!indexStatsState?.inFlight,
      loaded: !!indexStatsState?.valid,
      lastError: indexStatsState?.lastError,
      stats: selectIndexStats(database, table, indexUsageStats),
      lastReset,
    },
  };
};

export const mapDispatchToProps = (
  dispatch: Dispatch,
): DatabaseTablePageActions => ({
  refreshTableDetails: (database: string, table: string) => {
    dispatch(tableDetailsActions.refresh({ database, table }));
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
