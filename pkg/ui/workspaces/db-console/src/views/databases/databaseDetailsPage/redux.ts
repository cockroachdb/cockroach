// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { RouteComponentProps } from "react-router";
import { LocalSetting } from "src/redux/localsettings";
import {
  DatabaseDetailsPageData,
  defaultFilters,
  Filters,
  ViewMode,
  deriveTableDetailsMemoized,
} from "@cockroachlabs/cluster-ui";

import {
  refreshDatabaseDetails,
  refreshNodes,
  refreshTableDetails,
} from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { databaseNameAttr } from "src/util/constants";
import { getMatchParamByName } from "src/util/query";
import {
  nodeRegionsByIDSelector,
  selectIsMoreThanOneNode,
} from "src/redux/nodes";
import {
  selectDropUnusedIndexDuration,
  selectIndexRecommendationsEnabled,
} from "src/redux/clusterSettings";

const sortSettingTablesLocalSetting = new LocalSetting(
  "sortSetting/DatabasesDetailsTablesPage",
  (state: AdminUIState) => state.localSettings,
  { ascending: true, columnTitle: "name" },
);

const sortSettingGrantsLocalSetting = new LocalSetting(
  "sortSetting/DatabasesDetailsGrantsPage",
  (state: AdminUIState) => state.localSettings,
  { ascending: true, columnTitle: "name" },
);

// Hardcoded isTenant value for db-console.
const isTenant = false;

const viewModeLocalSetting = new LocalSetting(
  "viewMode/DatabasesDetailsPage",
  (state: AdminUIState) => state.localSettings,
  ViewMode.Tables,
);

const filtersLocalTablesSetting = new LocalSetting<AdminUIState, Filters>(
  "filters/DatabasesDetailsTablesPage",
  (state: AdminUIState) => state.localSettings,
  defaultFilters,
);

const searchLocalTablesSetting = new LocalSetting(
  "search/DatabasesDetailsTablesPage",
  (state: AdminUIState) => state.localSettings,
  null,
);

export const mapStateToProps = (
  state: AdminUIState,
  props: RouteComponentProps,
): DatabaseDetailsPageData => {
  const database = getMatchParamByName(props.match, databaseNameAttr);
  const databaseDetails = state?.cachedData.databaseDetails;
  const tableDetails = state?.cachedData.tableDetails;
  const dbTables =
    databaseDetails[database]?.data?.results.tablesResp.tables || [];
  const nodeRegions = nodeRegionsByIDSelector(state);
  const nodeStatuses = state?.cachedData.nodes.data;

  return {
    loading: !!databaseDetails[database]?.inFlight,
    loaded: !!databaseDetails[database]?.valid,
    requestError: databaseDetails[database]?.lastError,
    queryError: databaseDetails[database]?.data?.results?.error,
    name: database,
    showNodeRegionsColumn: selectIsMoreThanOneNode(state),
    viewMode: viewModeLocalSetting.selector(state),
    sortSettingTables: sortSettingTablesLocalSetting.selector(state),
    sortSettingGrants: sortSettingGrantsLocalSetting.selector(state),
    filters: filtersLocalTablesSetting.selector(state),
    search: searchLocalTablesSetting.selector(state),
    nodeRegions,
    isTenant,
    tables: deriveTableDetailsMemoized({
      dbName: database,
      tables: dbTables,
      tableDetails,
      nodeRegions,
      isTenant,
      nodeStatuses,
    }),
    showIndexRecommendations: selectIndexRecommendationsEnabled(state),
    csIndexUnusedDuration: selectDropUnusedIndexDuration(state),
  };
};

export const mapDispatchToProps = {
  refreshDatabaseDetails: (database: string, csIndexUnusedDuration: string) => {
    return refreshDatabaseDetails({
      database,
      csIndexUnusedDuration,
    });
  },
  refreshTableDetails: (
    database: string,
    table: string,
    csIndexUnusedDuration: string,
  ) => {
    return refreshTableDetails({
      database,
      table,
      csIndexUnusedDuration,
    });
  },
  onViewModeChange: (viewMode: ViewMode) => viewModeLocalSetting.set(viewMode),
  onSortingTablesChange: (columnName: string, ascending: boolean) =>
    sortSettingTablesLocalSetting.set({
      ascending: ascending,
      columnTitle: columnName,
    }),
  onSortingGrantsChange: (columnName: string, ascending: boolean) =>
    sortSettingGrantsLocalSetting.set({
      ascending: ascending,
      columnTitle: columnName,
    }),
  onSearchComplete: (query: string) => searchLocalTablesSetting.set(query),
  onFilterChange: (filters: Filters) => filtersLocalTablesSetting.set(filters),
  refreshNodes,
};
