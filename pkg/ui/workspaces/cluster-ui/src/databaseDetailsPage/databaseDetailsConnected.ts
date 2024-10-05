// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { connect } from "react-redux";
import { withRouter, RouteComponentProps } from "react-router-dom";
import { Dispatch } from "redux";
import { databaseNameCCAttr } from "src/util/constants";
import { getMatchParamByName } from "src/util/query";
import { AppState } from "../store";
import { databaseDetailsReducer } from "../store/databaseDetails";
const databaseDetailsActions = databaseDetailsReducer.actions;
import {
  actions as localStorageActions,
  LocalStorageKeys,
} from "../store/localStorage";
import { actions as tableDetailsActions } from "../store/databaseTableDetails";
import {
  DatabaseDetailsPage,
  DatabaseDetailsPageActions,
  DatabaseDetailsPageData,
} from "./databaseDetailsPage";
import { actions as analyticsActions } from "../store/analytics";
import { Filters } from "../queryFilter";
import { nodeRegionsByIDSelector } from "../store/nodes";
import { selectIsTenant } from "../store/uiConfig";
import {
  selectDatabaseDetailsGrantsSortSetting,
  selectDatabaseDetailsTablesFiltersSetting,
  selectDatabaseDetailsTablesSearchSetting,
  selectDatabaseDetailsTablesSortSetting,
  selectDatabaseDetailsViewModeSetting,
} from "../store/databaseDetails/databaseDetails.selectors";
import { deriveTableDetailsMemoized } from "../databases";
import {
  selectDropUnusedIndexDuration,
  selectIndexRecommendationsEnabled,
} from "../store/clusterSettings/clusterSettings.selectors";
import { actions as nodesActions } from "../store/nodes/nodes.reducer";
import { ViewMode } from "./types";

const mapStateToProps = (
  state: AppState,
  props: RouteComponentProps,
): DatabaseDetailsPageData => {
  const database = getMatchParamByName(props.match, databaseNameCCAttr);
  const databaseDetails = state.adminUI?.databaseDetails;
  const dbTables =
    databaseDetails[database]?.data?.results.tablesResp.tables || [];
  const nodeRegions = nodeRegionsByIDSelector(state);
  const isTenant = selectIsTenant(state);
  const nodeStatuses = state.adminUI?.nodes.data;
  return {
    loading: !!databaseDetails[database]?.inFlight,
    loaded: !!databaseDetails[database]?.valid,
    requestError: databaseDetails[database]?.lastError,
    queryError: databaseDetails[database]?.data?.results?.error,
    name: database,
    showNodeRegionsColumn: Object.keys(nodeRegions).length > 1 && !isTenant,
    viewMode: selectDatabaseDetailsViewModeSetting(state),
    sortSettingTables: selectDatabaseDetailsTablesSortSetting(state),
    sortSettingGrants: selectDatabaseDetailsGrantsSortSetting(state),
    filters: selectDatabaseDetailsTablesFiltersSetting(state),
    search: selectDatabaseDetailsTablesSearchSetting(state),
    nodeRegions,
    isTenant,
    tables: deriveTableDetailsMemoized({
      dbName: database,
      tables: dbTables,
      tableDetails: state.adminUI?.tableDetails,
      nodeRegions,
      isTenant,
      nodeStatuses,
    }),
    showIndexRecommendations: selectIndexRecommendationsEnabled(state),
    csIndexUnusedDuration: selectDropUnusedIndexDuration(state),
  };
};

const mapDispatchToProps = (
  dispatch: Dispatch,
): DatabaseDetailsPageActions => ({
  refreshDatabaseDetails: (database: string, csIndexUnusedDuration: string) => {
    dispatch(
      databaseDetailsActions.refresh({ database, csIndexUnusedDuration }),
    );
  },
  refreshTableDetails: (
    database: string,
    table: string,
    csIndexUnusedDuration: string,
  ) => {
    dispatch(
      tableDetailsActions.refresh({ database, table, csIndexUnusedDuration }),
    );
  },
  onViewModeChange: (viewMode: ViewMode) => {
    dispatch(
      analyticsActions.track({
        name: "View Mode Clicked",
        page: "Database Details",
        value: ViewMode[viewMode],
      }),
    );
    dispatch(
      localStorageActions.update({
        key: LocalStorageKeys.DB_DETAILS_VIEW_MODE,
        value: viewMode,
      }),
    );
  },
  onSortingTablesChange: (columnName: string, ascending: boolean) => {
    dispatch(
      analyticsActions.track({
        name: "Column Sorted",
        page: "Database Details",
        tableName: "Database Details",
        columnName,
      }),
    );
    dispatch(
      localStorageActions.update({
        key: LocalStorageKeys.DB_DETAILS_TABLES_PAGE_SORT,
        value: { columnTitle: columnName, ascending: ascending },
      }),
    );
  },
  onSortingGrantsChange: (columnName: string, ascending: boolean) => {
    dispatch(
      analyticsActions.track({
        name: "Column Sorted",
        page: "Database Details",
        tableName: "Database Details",
        columnName,
      }),
    );
    dispatch(
      localStorageActions.update({
        key: LocalStorageKeys.DB_DETAILS_GRANTS_PAGE_SORT,
        value: { columnTitle: columnName, ascending: ascending },
      }),
    );
  },
  onSearchComplete: (query: string) => {
    dispatch(
      analyticsActions.track({
        name: "Keyword Searched",
        page: "Database Details",
      }),
    );
    dispatch(
      localStorageActions.update({
        key: LocalStorageKeys.DB_DETAILS_TABLES_PAGE_SEARCH,
        value: query,
      }),
    );
  },
  onFilterChange: (filters: Filters) => {
    dispatch(
      analyticsActions.track({
        name: "Filter Clicked",
        page: "Database Details",
        filterName: "filters",
        value: filters.toString(),
      }),
    );
    dispatch(
      localStorageActions.update({
        key: LocalStorageKeys.DB_DETAILS_TABLES_PAGE_FILTERS,
        value: filters,
      }),
    );
  },
  refreshNodes: () => {
    dispatch(nodesActions.refresh());
  },
});

export const ConnectedDatabaseDetailsPage = withRouter(
  connect<
    DatabaseDetailsPageData,
    DatabaseDetailsPageActions,
    RouteComponentProps
  >(
    mapStateToProps,
    mapDispatchToProps,
  )(DatabaseDetailsPage),
);
