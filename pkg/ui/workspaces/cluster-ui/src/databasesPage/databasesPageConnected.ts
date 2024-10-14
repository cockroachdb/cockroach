// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { connect } from "react-redux";
import { withRouter } from "react-router-dom";
import { Dispatch } from "redux";

import { deriveDatabaseDetailsMemoized } from "../databases";
import { Filters } from "../queryFilter";
import { AppState } from "../store";
import { actions as analyticsActions } from "../store/analytics";
import { actions as clusterSettingsActions } from "../store/clusterSettings";
import {
  selectAutomaticStatsCollectionEnabled,
  selectDropUnusedIndexDuration,
  selectIndexRecommendationsEnabled,
} from "../store/clusterSettings/clusterSettings.selectors";
import {
  databaseDetailsReducer,
  databaseDetailsSpanStatsReducer,
} from "../store/databaseDetails";
import { actions as databasesListActions } from "../store/databasesList";
import {
  databasesListSelector,
  selectDatabasesFilters,
  selectDatabasesSearch,
  selectDatabasesSortSetting,
} from "../store/databasesList/databasesList.selectors";
import {
  actions as localStorageActions,
  LocalStorageKeys,
} from "../store/localStorage";
import {
  actions as nodesActions,
  nodeRegionsByIDSelector,
} from "../store/nodes";
import { selectIsTenant } from "../store/uiConfig";

import {
  DatabasesPage,
  DatabasesPageActions,
  DatabasesPageData,
} from "./databasesPage";

const databaseDetailsActions = databaseDetailsReducer.actions;
const databaseDetailsSpanStatsActions = databaseDetailsSpanStatsReducer.actions;

const mapStateToProps = (state: AppState): DatabasesPageData => {
  const databasesListState = databasesListSelector(state);
  const nodeRegions = nodeRegionsByIDSelector(state);
  const isTenant = selectIsTenant(state);
  return {
    loading: !!databasesListState?.inFlight,
    loaded: !!databasesListState?.valid,
    requestError: databasesListState?.lastError,
    queryError: databasesListState?.data?.error,
    databases: deriveDatabaseDetailsMemoized({
      dbListResp: databasesListState?.data,
      databaseDetails: state.adminUI?.databaseDetails,
      spanStats: state.adminUI?.databaseDetailsSpanStats,
      nodeRegions,
      isTenant,
      nodeStatuses: state.adminUI.nodes.data,
    }),
    sortSetting: selectDatabasesSortSetting(state),
    search: selectDatabasesSearch(state),
    filters: selectDatabasesFilters(state),
    nodeRegions,
    isTenant,
    automaticStatsCollectionEnabled:
      selectAutomaticStatsCollectionEnabled(state),
    // Do not show node/regions columns for serverless.
    indexRecommendationsEnabled: selectIndexRecommendationsEnabled(state),
    showNodeRegionsColumn: Object.keys(nodeRegions).length > 1 && !isTenant,
    csIndexUnusedDuration: selectDropUnusedIndexDuration(state),
  };
};

const mapDispatchToProps = (dispatch: Dispatch): DatabasesPageActions => ({
  refreshDatabases: () => {
    dispatch(databasesListActions.refresh());
  },
  refreshDatabaseDetails: (database: string, csIndexUnusedDuration: string) => {
    dispatch(
      databaseDetailsActions.refresh({ database, csIndexUnusedDuration }),
    );
  },
  refreshDatabaseSpanStats: (database: string) => {
    dispatch(databaseDetailsSpanStatsActions.refresh({ database }));
  },
  refreshSettings: () => {
    dispatch(clusterSettingsActions.refresh());
  },
  refreshNodes: () => {
    dispatch(nodesActions.refresh());
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
        key: LocalStorageKeys.DB_FILTERS,
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
        key: LocalStorageKeys.DB_SEARCH,
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
        key: LocalStorageKeys.DB_SORT,
        value: { columnTitle: columnName, ascending: ascending },
      }),
    );
  },
});

export const ConnectedDatabasesPage = withRouter(
  connect(mapStateToProps, mapDispatchToProps)(DatabasesPage),
);
