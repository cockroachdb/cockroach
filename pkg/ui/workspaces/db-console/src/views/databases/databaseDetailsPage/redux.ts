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
  util,
  ViewMode,
} from "@cockroachlabs/cluster-ui";

import { cockroach } from "src/js/protos";
import {
  generateTableID,
  refreshDatabaseDetails,
  refreshTableDetails,
  refreshTableStats,
} from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { databaseNameAttr } from "src/util/constants";
import { FixLong } from "src/util/fixLong";
import { getMatchParamByName } from "src/util/query";
import {
  nodeRegionsByIDSelector,
  selectIsMoreThanOneNode,
} from "src/redux/nodes";
import { getNodesByRegionString, normalizePrivileges } from "../utils";

const { TableDetailsRequest, TableStatsRequest } = cockroach.server.serverpb;

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
  state => state.cachedData.tableStats,
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
    tableStats,
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
      lastError: databaseDetails[database]?.lastError,
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
        databaseDetails[database]?.data?.tables_resp.tables,
        tableRow => {
          const table = tableRow.table_name;
          const tableId = generateTableID(database, table);

          const details = tableDetails[tableId];
          const stats = tableStats[tableId];

          const roles = normalizeRoles(_.map(details?.data?.grants, "user"));
          const grants = normalizePrivileges(
            _.flatMap(details?.data?.grants, "privileges"),
          );
          const nodes = stats?.data?.node_ids || [];
          const numIndexes = _.uniq(
            _.map(details?.data?.indexes, index => index.name),
          ).length;
          return {
            name: table,
            details: {
              loading: !!details?.inFlight,
              loaded: !!details?.valid,
              lastError: details?.lastError,
              columnCount: details?.data?.columns?.length || 0,
              indexCount: numIndexes,
              userCount: roles.length,
              roles: roles,
              grants: grants,
              statsLastUpdated: details?.data?.stats_last_created_at
                ? util.TimestampToMoment(details?.data?.stats_last_created_at)
                : null,
              hasIndexRecommendations:
                details?.data?.has_index_recommendations || false,
              totalBytes: FixLong(
                details?.data?.data_total_bytes || 0,
              ).toNumber(),
              liveBytes: FixLong(
                details?.data?.data_live_bytes || 0,
              ).toNumber(),
              livePercentage: details?.data?.data_live_percentage || 0,
            },
            stats: {
              loading: !!stats?.inFlight,
              loaded: !!stats?.valid,
              lastError: stats?.lastError,
              replicationSizeInBytes: FixLong(
                stats?.data?.approximate_disk_bytes || 0,
              ).toNumber(),
              nodes: nodes,
              rangeCount: FixLong(stats?.data?.range_count || 0).toNumber(),
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
    return refreshTableDetails(new TableDetailsRequest({ database, table }));
  },
  refreshTableStats: (database: string, table: string) => {
    return refreshTableStats(new TableStatsRequest({ database, table }));
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
