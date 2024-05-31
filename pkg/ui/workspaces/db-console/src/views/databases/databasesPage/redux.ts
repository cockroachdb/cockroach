// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { createSelector } from "reselect";
import {
  DatabasesPageData,
  defaultFilters,
  Filters,
  deriveDatabaseDetailsMemoized,
} from "@cockroachlabs/cluster-ui";

import { LocalSetting } from "src/redux/localsettings";
import {
  refreshDatabases,
  refreshDatabaseDetails,
  refreshNodes,
  refreshSettings,
  refreshDatabaseDetailsSpanStats,
} from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import {
  nodeRegionsByIDSelector,
  selectIsMoreThanOneNode,
} from "src/redux/nodes";
import {
  selectAutomaticStatsCollectionEnabled,
  selectDropUnusedIndexDuration,
  selectIndexRecommendationsEnabled,
} from "src/redux/clusterSettings";

const selectLoading = createSelector(
  (state: AdminUIState) => state.cachedData.databases,
  databases => databases.inFlight,
);

const selectLoaded = createSelector(
  (state: AdminUIState) => state.cachedData.databases,
  databases => databases.valid,
);

const selectLastError = createSelector(
  (state: AdminUIState) => state.cachedData.databases,
  databases => databases.lastError,
);

// Hardcoded isTenant value for db-console.
const isTenant = false;

const sortSettingLocalSetting = new LocalSetting(
  "sortSetting/DatabasesPage",
  (state: AdminUIState) => state.localSettings,
  { ascending: true, columnTitle: "name" },
);

const filtersLocalSetting = new LocalSetting<AdminUIState, Filters>(
  "filters/DatabasesPage",
  (state: AdminUIState) => state.localSettings,
  defaultFilters,
);

const searchLocalSetting = new LocalSetting(
  "search/DatabasesPage",
  (state: AdminUIState) => state.localSettings,
  null,
);

export const mapStateToProps = (state: AdminUIState): DatabasesPageData => {
  const dbListResp = state?.cachedData.databases.data;
  const databaseDetails = state?.cachedData.databaseDetails;
  const spanStats = state?.cachedData.databaseDetailsSpanStats;
  const nodeRegions = nodeRegionsByIDSelector(state);
  return {
    loading: selectLoading(state),
    loaded: selectLoaded(state),
    requestError: selectLastError(state),
    queryError: dbListResp?.error,
    databases: deriveDatabaseDetailsMemoized({
      dbListResp,
      databaseDetails,
      spanStats,
      nodeRegions,
      isTenant,
      nodeStatuses: state?.cachedData.nodes.data,
    }),
    sortSetting: sortSettingLocalSetting.selector(state),
    filters: filtersLocalSetting.selector(state),
    search: searchLocalSetting.selector(state),
    nodeRegions,
    isTenant,
    automaticStatsCollectionEnabled:
      selectAutomaticStatsCollectionEnabled(state),
    indexRecommendationsEnabled: selectIndexRecommendationsEnabled(state),
    showNodeRegionsColumn: selectIsMoreThanOneNode(state),
    csIndexUnusedDuration: selectDropUnusedIndexDuration(state),
  };
};

export const mapDispatchToProps = {
  refreshSettings,
  refreshDatabases,
  refreshDatabaseDetails: (database: string, csIndexUnusedDuration: string) => {
    return refreshDatabaseDetails({
      database,
      csIndexUnusedDuration,
    });
  },
  refreshDatabaseSpanStats: (database: string) => {
    return refreshDatabaseDetailsSpanStats({ database });
  },
  refreshNodes,
  onSortingChange: (
    _tableName: string,
    columnName: string,
    ascending: boolean,
  ) =>
    sortSettingLocalSetting.set({
      ascending: ascending,
      columnTitle: columnName,
    }),
  onSearchComplete: (query: string) => searchLocalSetting.set(query),
  onFilterChange: (filters: Filters) => filtersLocalSetting.set(filters),
};
