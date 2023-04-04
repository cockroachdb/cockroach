// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import {RouteComponentProps} from "react-router";
import {createSelector} from "reselect";
import {databaseNameAttr} from "src/util/constants";
import {getMatchParamByName} from "src/util/query";
import {AppState} from "../store";
import {Dispatch} from "redux";
import {actions as databaseDetailsActions} from "../store/databaseDetails";
import {actions as localStorageActions, LocalStorageKeys} from "../store/localStorage";
import {actions as tableDetailsActions} from "../store/databaseTableDetails";
import {
  DatabaseDetailsPageActions,
  DatabaseDetailsPageData,
  DatabaseDetailsPageDataTable,
  ViewMode
} from "./databaseDetailsPage";
import {actions as analyticsActions} from "../store/analytics";
import {Filters} from "../queryFilter";
import {nodeRegionsByIDSelector} from "../store/nodes";
import {selectIsTenant} from "../store/uiConfig";
import {
  selectDatabaseDetails,
  selectDatabaseDetailsGrantsSortSetting,
  selectDatabaseDetailsTablesFiltersSetting,
  selectDatabaseDetailsTablesSearchSetting,
  selectDatabaseDetailsTablesSortSetting,
  selectDatabaseDetailsViewModeSetting
} from "../store/databaseDetails/databaseDetails.selectors";
import {
  combineLoadingErrors,
  generateTableID,
  getNodesByRegionString,
  normalizePrivileges,
  normalizeRoles
} from "../util";
import {selectTableDetails} from "../store/databaseTableDetails/tableDetails.selector";

const selectTables = (state: AppState, props: RouteComponentProps): DatabaseDetailsPageDataTable[] => {
  const database = getMatchParamByName(props.match, databaseNameAttr);
  const dbDetails = selectDatabaseDetails(state);
  const tableDetails = selectTableDetails(state);
  const dbTables = dbDetails.cache[database]?.data?.results.tablesResp.tables;

  return dbTables.map(table => {
    const tableId = generateTableID(database, table);
    const details = tableDetails.cache[tableId];
    const roles = normalizeRoles(details?.data?.results.grantsResp.grants.map(grant => grant["user"]));
    const grants = normalizePrivileges([].concat(...details?.data?.results.grantsResp.grants.map(grant => grant["privileges"])));
    const nodes = details?.data?.results.stats.replicaData.nodeIDs || [];
    // Query fetches distinct table indexes.
    const numIndexes = details?.data?.results.schemaDetails.indexes.length;
    return {
      name: table,
      loading: !!details?.inFlight,
      loaded: !!details?.valid,
      lastError: details?.lastError,
      details: {
        columnCount:
          details?.data?.results.schemaDetails.columns?.length || 0,
        indexCount: numIndexes,
        userCount: roles.length,
        roles: roles,
        grants: grants,
        statsLastUpdated:
          details?.data?.results.heuristicsDetails
            .stats_last_created_at || null,
        hasIndexRecommendations:
          details?.data?.results.stats.indexStats
            .has_index_recommendations || false,
        totalBytes:
          details?.data?.results.stats.spanStats.total_bytes || 0,
        liveBytes: details?.data?.results.stats.spanStats.live_bytes || 0,
        livePercentage:
          details?.data?.results.stats.spanStats.live_percentage || 0,
        replicationSizeInBytes:
          details?.data?.results.stats.spanStats.approximate_disk_bytes ||
          0,
        nodes: nodes,
        rangeCount:
          details?.data?.results.stats.spanStats.range_count || 0,
        nodesByRegionString: getNodesByRegionString(
          nodes,
          nodeRegionsByIDSelector(state),
          selectIsTenant(state),
        ),
      },
    };
  }) || [];
};

export const mapStateToProps = createSelector(
  (_state: AppState, props: RouteComponentProps): string =>
    getMatchParamByName(props.match, databaseNameAttr),
  state => selectDatabaseDetails(state),
  state => nodeRegionsByIDSelector(state),
  state => selectDatabaseDetailsViewModeSetting(state),
  state => selectDatabaseDetailsTablesSortSetting(state),
  state => selectDatabaseDetailsGrantsSortSetting(state),
  state => selectDatabaseDetailsTablesFiltersSetting(state),
  state => selectDatabaseDetailsTablesSearchSetting(state),
    state => selectIsTenant(state),
  (state: AppState, props: RouteComponentProps) => selectTables(state, props),
  (
    database,
    databaseDetails,
    nodeRegions,
    viewMode,
    sortSettingTables,
    sortSettingGrants,
    filtersLocalTables,
    searchLocalTables,
    isTenant,
    tables,
  ): DatabaseDetailsPageData => {
    return {
      loading: !!databaseDetails.cache[database]?.inFlight,
      loaded: !!databaseDetails.cache[database]?.valid,
      lastError: combineLoadingErrors(
        databaseDetails.cache[database]?.lastError,
        databaseDetails.cache[database]?.data?.maxSizeReached,
        null,
      ),
      name: database,
      // Default setting this to true. On db-console we only display this when
      // we have >1 node, but I'm not sure why.
      showNodeRegionsColumn: true,
      viewMode,
      sortSettingTables,
      sortSettingGrants,
      filters: filtersLocalTables,
      search: searchLocalTables,
      nodeRegions,
      isTenant,
      tables,
    };
  },
);

export const mapDispatchToProps = (dispatch: Dispatch): DatabaseDetailsPageActions => ({
  refreshDatabaseDetails: (database: string) => {
    dispatch(databaseDetailsActions.refresh(database))
  },
  refreshTableDetails: (database: string, table: string) => {
    dispatch(tableDetailsActions.refresh({database, table}))
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
