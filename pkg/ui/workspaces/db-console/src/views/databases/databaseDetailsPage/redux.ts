// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { RouteComponentProps } from "react-router";
import { createSelector } from "reselect";
import { LocalSetting } from "src/redux/localsettings";
import _ from "lodash";
import {
  DatabaseDetailsPageData,
  defaultFilters,
  Filters,
  ViewMode,
} from "@cockroachlabs/cluster-ui";

import {
  generateTableID,
  refreshDatabaseDetails,
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
  combineLoadingErrors,
  getNodesByRegionString,
  normalizePrivileges,
} from "../utils";

function normalizeRoles(raw: string[]): string[] {
  const rolePrecedence: Record<string, number> = {
    root: 1,
    admin: 2,
    public: 3,
  };

  // Once we have an alphabetized list of roles, we sort it again, promoting
  // root, admin, and public to the head of the list. (We rely on _.sortBy to
  // be stable.)
  const alphabetizedRoles = _.sortBy(_.uniq(_.filter(raw)));

  return _.sortBy(alphabetizedRoles, role => rolePrecedence[role] || 100);
}

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

export const mapStateToProps = createSelector(
  (_state: AdminUIState, props: RouteComponentProps): string =>
    getMatchParamByName(props.match, databaseNameAttr),

  state => state.cachedData.databaseDetails,
  state => state.cachedData.tableDetails,
  state => nodeRegionsByIDSelector(state),
  state => selectIsMoreThanOneNode(state),
  state => viewModeLocalSetting.selector(state),
  state => sortSettingTablesLocalSetting.selector(state),
  state => sortSettingGrantsLocalSetting.selector(state),
  state => filtersLocalTablesSetting.selector(state),
  state => searchLocalTablesSetting.selector(state),
  (_: AdminUIState) => isTenant,
  (
    database,
    databaseDetails,
    tableDetails,
    nodeRegions,
    showNodeRegionsColumn,
    viewMode,
    sortSettingTables,
    sortSettingGrants,
    filtersLocalTables,
    searchLocalTables,
    isTenant,
  ): DatabaseDetailsPageData => {
    return {
      loading: !!databaseDetails[database]?.inFlight,
      loaded: !!databaseDetails[database]?.valid,
      lastError: combineLoadingErrors(
        databaseDetails[database]?.lastError,
        databaseDetails[database]?.data?.maxSizeReached,
        null,
      ),
      name: database,
      showNodeRegionsColumn,
      viewMode,
      sortSettingTables,
      sortSettingGrants,
      filters: filtersLocalTables,
      search: searchLocalTables,
      nodeRegions: nodeRegions,
      isTenant: isTenant,
      tables: _.map(
        databaseDetails[database]?.data?.results.tablesResp.tables,
        table => {
          const tableId = generateTableID(database, table);
          const details = tableDetails[tableId];

          const roles = normalizeRoles(
            _.map(details?.data?.results.grantsResp.grants, "user"),
          );
          const grants = normalizePrivileges(
            _.flatMap(details?.data?.results.grantsResp.grants, "privileges"),
          );
          const nodes = details?.data?.results.stats.replicaData.nodeIDs || [];
          const numIndexes = _.uniq(
            details?.data?.results.schemaDetails.indexes,
          ).length;
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
                nodeRegions,
                isTenant,
              ),
            },
          };
        },
      ),
    };
  },
);

export const mapDispatchToProps = {
  refreshDatabaseDetails,
  refreshTableDetails: (database: string, table: string) => {
    return refreshTableDetails({
      database,
      table,
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
};
