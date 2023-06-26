// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import _ from "lodash";
import { createSelector } from "reselect";
import { LocalSetting } from "src/redux/localsettings";
import {
  DatabasesPageData,
  DatabasesPageDataDatabase,
  defaultFilters,
  Filters,
} from "@cockroachlabs/cluster-ui";

import { cockroach } from "src/js/protos";
import {
  generateTableID,
  refreshDatabaseDetails,
  refreshDatabases,
  refreshNodes,
  refreshSettings,
  refreshTableStats,
} from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { FixLong } from "src/util/fixLong";
import {
  nodeRegionsByIDSelector,
  selectIsMoreThanOneNode,
} from "src/redux/nodes";
import { getNodesByRegionString } from "../utils";
import { selectAutomaticStatsCollectionEnabled } from "src/redux/clusterSettings";

const { DatabaseDetailsRequest, TableStatsRequest } = cockroach.server.serverpb;

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
  (state: AdminUIState) => state.cachedData.databases,
  (state: AdminUIState) => state.cachedData.databaseDetails,
  (state: AdminUIState) => state.cachedData.tableStats,
  (state: AdminUIState) => nodeRegionsByIDSelector(state),
  (_: AdminUIState) => isTenant,
  (
    databases,
    databaseDetails,
    tableStats,
    nodeRegions,
    isTenant,
  ): DatabasesPageDataDatabase[] =>
    (databases?.data?.databases || []).map(database => {
      const details = databaseDetails[database];

      const stats = details?.data?.stats;
      let sizeInBytes = FixLong(stats?.approximate_disk_bytes || 0).toNumber();
      let rangeCount = FixLong(stats?.range_count || 0).toNumber();
      const nodes = stats?.node_ids || [];

      // We offer the component a chance to refresh any table-level stats we
      // weren't able to gather during the initial database details call, by
      // exposing a list of "missing tables."
      //
      // Furthermore, when the database-level stats are completely absent
      // from the database details response (perhaps we're talking to an
      // older backend that doesn't support them), we mark _all_ the tables
      // as "missing", so that the component can trigger refresh calls for
      // all of their individual stats.

      const possiblyMissingTables = stats
        ? stats.missing_tables.map(table => table.name)
        : details?.data?.table_names;

      const [individuallyLoadedTables, missingTables] = _.partition(
        possiblyMissingTables,
        table => {
          return !!tableStats[generateTableID(database, table)]?.valid;
        },
      );

      individuallyLoadedTables.forEach(table => {
        const stats = tableStats[generateTableID(database, table)];
        sizeInBytes += FixLong(
          stats?.data?.approximate_disk_bytes || 0,
        ).toNumber();
        rangeCount += FixLong(stats?.data?.range_count || 0).toNumber();
      });

      const nodesByRegionString = getNodesByRegionString(
        nodes,
        nodeRegions,
        isTenant,
      );
      const numIndexRecommendations = stats?.num_index_recommendations || 0;

      const combinedErr = combineLoadingErrors(
        details?.lastError,
        databases?.lastError,
      );

      return {
        loading: !!details?.inFlight,
        loaded: !!details?.valid,
        lastError: combinedErr,
        name: database,
        sizeInBytes: sizeInBytes,
        tableCount: details?.data?.table_names?.length || 0,
        rangeCount: rangeCount,
        nodes: nodes,
        nodesByRegionString,
        numIndexRecommendations,
        missingTables: missingTables.map(table => {
          return {
            loading: !!tableStats[generateTableID(database, table)]?.inFlight,
            name: table,
          };
        }),
      };
    }),
);

function combineLoadingErrors(detailsErr: Error, dbListErr: Error): Error {
  if (!dbListErr) {
    return detailsErr;
  }

  if (!detailsErr) {
    return new GetDatabaseInfoError(
      `Failed to load all databases. Partial results are shown. Debug info: ${dbListErr}`,
    );
  }

  return new GetDatabaseInfoError(
    `Failed to load all databases and database details. Partial results are shown. Debug info: ${dbListErr}, details error: ${detailsErr}`,
  );
}

export class GetDatabaseInfoError extends Error {
  constructor(message: string) {
    super(message);

    this.name = this.constructor.name;
  }
}

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
  refreshDatabaseDetails: (database: string) => {
    return refreshDatabaseDetails(
      new DatabaseDetailsRequest({ database, include_stats: true }),
    );
  },
  refreshTableStats: (database: string, table: string) => {
    return refreshTableStats(new TableStatsRequest({ database, table }));
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
