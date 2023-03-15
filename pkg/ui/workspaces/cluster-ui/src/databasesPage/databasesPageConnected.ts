// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import {AppState} from "../store";
import {withRouter} from "react-router-dom";
import {DatabasesPage, DatabasesPageActions, DatabasesPageData, DatabasesPageDataDatabase,} from "./databasesPage";
import {connect} from "react-redux";
import {
  databasesListSelector,
  selectDatabasesFilters,
  selectDatabasesSearch,
  selectDatabasesSortSetting,
} from "../store/databasesList/databasesList.selectors";
import {actions as nodesActions, nodeRegionsByIDSelector} from "../store/nodes";
import {selectIsTenant} from "../store/uiConfig";
import {Dispatch} from "redux";
import {actions as clusterSettingsActions} from "../store/clusterSettings";
import {actions as databasesListActions} from "../store/databasesList";
import {actions as databaseDetailsActions} from "../store/databaseDetails";
import {actions as localStorageActions} from "../store/localStorage";
import {Filters} from "../queryFilter";
import {actions as analyticsActions} from "../store/analytics";
import {selectAutomaticStatsCollectionEnabled} from "../store/clusterSettings/clusterSettings.selectors";
import {combineLoadingErrors, getNodesByRegionString} from "../util";

const selectDatabases = (state: AppState): DatabasesPageDataDatabase[] => {
  const dbListResp = databasesListSelector(state).data;
  const databases = dbListResp?.databases || [];
  return databases.map(dbName => {
    const dbDetails = state.adminUI?.databaseDetails.cache[dbName];
    const dbStats = dbDetails?.data?.results.stats;
    const sizeInBytes = dbStats?.spanStats?.approximate_disk_bytes || 0;
    const rangeCount = dbStats?.spanStats.range_count || 0;
    const nodes = dbStats?.replicaData.replicas || [];
    const nodesByRegionString = getNodesByRegionString(
      nodes,
      nodeRegionsByIDSelector(state),
      selectIsTenant(state),
    );
    const numIndexRecommendations =
      dbStats?.indexStats.num_index_recommendations || 0;

    const combinedErr = combineLoadingErrors(
      dbDetails?.lastError,
      dbDetails?.data?.maxSizeReached,
      dbListResp?.error?.message,
    );

    return {
      loading: !!dbDetails?.inFlight,
      loaded: !!dbDetails?.valid,
      lastError: combinedErr,
      name: dbName,
      sizeInBytes: sizeInBytes,
      tableCount: dbDetails?.data?.results.tablesResp.tables?.length || 0,
      rangeCount: rangeCount,
      nodes: nodes,
      nodesByRegionString,
      numIndexRecommendations,
    };
  });
}

const mapStateToProps = (state: AppState): DatabasesPageData => {
  const databasesListState = databasesListSelector(state);
  return {
    loading: databasesListState.inFlight,
    loaded: databasesListState.valid,
    lastError:
      databasesListState.lastError,
    databases: selectDatabases(state),
    sortSetting: selectDatabasesSortSetting(state),
    search: selectDatabasesSearch(state),
    filters: selectDatabasesFilters(state),
    nodeRegions: nodeRegionsByIDSelector(state),
    isTenant: selectIsTenant(state),
    automaticStatsCollectionEnabled: selectAutomaticStatsCollectionEnabled(state),
    // Default setting this to true. On db-console we only display this when
    // we have >1 node, but I'm not sure why.
    showNodeRegionsColumn: true,
  };
};

const mapDispatchToProps = (dispatch: Dispatch): DatabasesPageActions => ({
  refreshDatabases: () => {
    dispatch(databasesListActions.refresh())
  },
  refreshDatabaseDetails: (database: string) => {
    dispatch(databaseDetailsActions.refresh(database))
  },
  refreshSettings: () => {
    dispatch(clusterSettingsActions.refresh())
  },
  refreshNodes: () => {
    dispatch(nodesActions.refresh())
  },
  onFilterChange: (filters: Filters) => {
    dispatch(
      analyticsActions.track({
        name: "Filter Clicked",
        page: "Databases",
        filterName: "filters",
        value: filters.toString(),
      }),
    );
    dispatch(
      localStorageActions.update({
        key: "filters/DatabasesPage",
        value: filters,
      }),
    );
  },
  onSearchComplete: (query: string) => {
    dispatch(
      analyticsActions.track({
        name: "Keyword Searched",
        page: "Databases",
      }),
    );
    dispatch(
      localStorageActions.update({
        key: "search/DatabasesPage",
        value: query,
      }),
    );
  },
  onSortingChange: (
    tableName: string,
    columnName: string,
    ascending: boolean,
  ) => {
    dispatch(
      analyticsActions.track({
        name: "Column Sorted",
        page: "Databases",
        tableName,
        columnName,
      }),
    );
    dispatch(
      localStorageActions.update({
        key: "sortSetting/DatabasesPage",
        value: { columnTitle: columnName, ascending: ascending },
      }),
    );
  },
});

export const ConnectedDatabasesPage = withRouter<any, any>(
  connect(mapStateToProps, mapDispatchToProps)(DatabasesPage),
);
