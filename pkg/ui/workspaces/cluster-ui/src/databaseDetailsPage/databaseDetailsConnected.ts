// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { connect } from "react-redux";
import { withRouter, RouteComponentProps } from "react-router-dom";
import { Dispatch } from "redux";
import { databaseNameCCAttr } from "src/util/constants";
import { getMatchParamByName } from "src/util/query";
import { AppState } from "../store";
import { actions as databaseDetailsActions } from "../store/databaseDetails";
import {
  actions as localStorageActions,
  LocalStorageKeys,
} from "../store/localStorage";
import { actions as tableDetailsActions } from "../store/databaseTableDetails";
import {
  DatabaseDetailsPage,
  DatabaseDetailsPageActions,
  DatabaseDetailsPageData,
  ViewMode,
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
import { combineLoadingErrors, deriveTableDetailsMemoized } from "../databases";
import { selectIndexRecommendationsEnabled } from "../store/clusterSettings/clusterSettings.selectors";

const mapStateToProps = (
  state: AppState,
  props: RouteComponentProps,
): DatabaseDetailsPageData => {
  const database = getMatchParamByName(props.match, databaseNameCCAttr);
  const databaseDetails = state.adminUI.databaseDetails;
  const dbTables =
    databaseDetails[database]?.data?.results.tablesResp.tables || [];
  const nodeRegions = nodeRegionsByIDSelector(state);
  const isTenant = selectIsTenant(state);
  return {
    loading: !!databaseDetails[database]?.inFlight,
    loaded: !!databaseDetails[database]?.valid,
    lastError: combineLoadingErrors(
      databaseDetails[database]?.lastError,
      databaseDetails[database]?.data?.maxSizeReached,
      null,
    ),
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
    }),
    showIndexRecommendations: selectIndexRecommendationsEnabled(state),
  };
};

const mapDispatchToProps = (
  dispatch: Dispatch,
): DatabaseDetailsPageActions => ({
  refreshDatabaseDetails: (database: string) => {
    dispatch(databaseDetailsActions.refresh(database));
  },
  refreshTableDetails: (database: string, table: string) => {
    dispatch(tableDetailsActions.refresh({ database, table }));
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
