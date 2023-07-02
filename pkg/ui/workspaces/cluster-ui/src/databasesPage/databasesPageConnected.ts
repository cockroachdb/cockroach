// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { AppState } from "../store";
import { withRouter } from "react-router-dom";
import {
  DatabasesPage,
  DatabasesPageActions,
  DatabasesPageData,
} from "./databasesPage";
import { connect } from "react-redux";
import {
  databasesListSelector,
  selectDatabasesFilters,
  selectDatabasesSearch,
  selectDatabasesSortSetting,
} from "../store/databasesList/databasesList.selectors";
import {
  actions as nodesActions,
  nodeRegionsByIDSelector,
} from "../store/nodes";
import { selectIsTenant } from "../store/uiConfig";
import { Dispatch } from "redux";
import { actions as clusterSettingsActions } from "../store/clusterSettings";
import { actions as databasesListActions } from "../store/databasesList";
import { actions as databaseDetailsActions } from "../store/databaseDetails";
import {
  actions as localStorageActions,
  LocalStorageKeys,
} from "../store/localStorage";
import { Filters } from "../queryFilter";
import { actions as analyticsActions } from "../store/analytics";
import {
  selectAutomaticStatsCollectionEnabled,
  selectIndexRecommendationsEnabled,
} from "../store/clusterSettings/clusterSettings.selectors";
import { deriveDatabaseDetailsMemoized } from "../databases";

const mapStateToProps = (state: AppState): DatabasesPageData => {
  const databasesListState = databasesListSelector(state);
  const nodeRegions = nodeRegionsByIDSelector(state);
  const isTenant = selectIsTenant(state);
  return {
    loading: !!databasesListState?.inFlight,
    loaded: !!databasesListState?.valid,
    lastError: databasesListState?.lastError,
    databases: deriveDatabaseDetailsMemoized({
      dbListResp: databasesListState?.data,
      databaseDetails: state.adminUI?.databaseDetails,
      nodeRegions,
      isTenant,
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
  };
};

const mapDispatchToProps = (dispatch: Dispatch): DatabasesPageActions => ({
  refreshDatabases: () => {
    dispatch(databasesListActions.refresh());
  },
  refreshDatabaseDetails: (database: string) => {
    dispatch(databaseDetailsActions.refresh(database));
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

export const ConnectedDatabasesPage = withRouter<any, any>(
  connect(mapStateToProps, mapDispatchToProps)(DatabasesPage),
);
