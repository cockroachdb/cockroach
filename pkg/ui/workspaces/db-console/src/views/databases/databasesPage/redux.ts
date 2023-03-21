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
import { LocalSetting } from "src/redux/localsettings";
import {
  DatabasesPageData,
  DatabasesPageDataDatabase,
  defaultFilters,
  Filters,
} from "@cockroachlabs/cluster-ui";

import {
  refreshDatabases,
  refreshDatabaseDetails,
  refreshNodes,
  refreshSettings,
} from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import {
  nodeRegionsByIDSelector,
  selectIsMoreThanOneNode,
} from "src/redux/nodes";
import { combineLoadingErrors, getNodesByRegionString } from "../utils";
import { selectAutomaticStatsCollectionEnabled } from "src/redux/clusterSettings";

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
  "search/DatabsesPage",
  (state: AdminUIState) => state.localSettings,
  null,
);

const selectDatabases = createSelector(
  (state: AdminUIState) => state.cachedData.databases.data,
  (state: AdminUIState) => state.cachedData.databaseDetails,
  (state: AdminUIState) => nodeRegionsByIDSelector(state),
  (_: AdminUIState) => isTenant,
  (
    databases,
    databaseDetails,
    nodeRegions,
    isTenant,
  ): DatabasesPageDataDatabase[] =>
    (databases?.databases || []).map(database => {
      const details = databaseDetails[database];
      const stats = details?.data?.results.stats;
      const sizeInBytes = stats?.spanStats?.approximate_disk_bytes || 0;
      const rangeCount = stats?.spanStats.range_count || 0;
      // TODO(thomas): Eventually, we should populate this will real node IDs.
      const nodes = stats?.replicaData.replicas || [];
      const nodesByRegionString = getNodesByRegionString(
        nodes,
        nodeRegions,
        isTenant,
      );
      const numIndexRecommendations =
        stats?.indexStats.num_index_recommendations || 0;

      const combinedErr = combineLoadingErrors(
        details?.lastError,
        details?.data?.maxSizeReached,
        databases?.error?.message,
      );

      return {
        loading: !!details?.inFlight,
        loaded: !!details?.valid,
        lastError: combinedErr,
        name: database,
        sizeInBytes: sizeInBytes,
        tableCount: details?.data?.results.tablesResp.tables?.length || 0,
        rangeCount: rangeCount,
        nodes: nodes,
        nodesByRegionString,
        numIndexRecommendations,
      };
    }),
);

export const mapStateToProps = (state: AdminUIState): DatabasesPageData => ({
  loading: selectLoading(state),
  loaded: selectLoaded(state),
  lastError: selectLastError(state),
  databases: selectDatabases(state),
  sortSetting: sortSettingLocalSetting.selector(state),
  filters: filtersLocalSetting.selector(state),
  search: searchLocalSetting.selector(state),
  nodeRegions: nodeRegionsByIDSelector(state),
  isTenant: isTenant,
  automaticStatsCollectionEnabled: selectAutomaticStatsCollectionEnabled(state),
  showNodeRegionsColumn: selectIsMoreThanOneNode(state),
});

export const mapDispatchToProps = {
  refreshSettings,
  refreshDatabases,
  refreshDatabaseDetails,
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
