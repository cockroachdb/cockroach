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
import { getNodesByRegionString } from "../utils";

const {
  DatabaseDetailsRequest,
  TableDetailsRequest,
  TableStatsRequest,
} = cockroach.server.serverpb;

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

function normalizePrivileges(raw: string[]): string[] {
  const privilegePrecedence: Record<string, number> = {
    ALL: 1,
    CREATE: 2,
    DROP: 3,
    GRANT: 4,
    SELECT: 5,
    INSERT: 6,
    UPDATE: 7,
    DELETE: 8,
  };

  return _.sortBy(
    _.uniq(_.map(_.filter(raw), _.toUpper)),
    privilege => privilegePrecedence[privilege] || 100,
  );
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

const viewModeLocalSetting = new LocalSetting(
  "viewMode/DatabasesDetailsPage",
  (state: AdminUIState) => state.localSettings,
  ViewMode.Tables,
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
  ): DatabaseDetailsPageData => {
    return {
      loading: !!databaseDetails[database]?.inFlight,
      loaded: !!databaseDetails[database]?.valid,
      name: database,
      showNodeRegionsColumn,
      viewMode,
      sortSettingTables,
      sortSettingGrants,
      tables: _.map(databaseDetails[database]?.data?.table_names, table => {
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
            columnCount: details?.data?.columns?.length || 0,
            indexCount: numIndexes,
            userCount: roles.length,
            roles: roles,
            grants: grants,
            statsLastUpdated: details?.data?.stats_last_created_at
              ? util.TimestampToMoment(details?.data?.stats_last_created_at)
              : null,
          },
          stats: {
            loading: !!stats?.inFlight,
            loaded: !!stats?.valid,
            replicationSizeInBytes: FixLong(
              stats?.data?.approximate_disk_bytes || 0,
            ).toNumber(),
            rangeCount: FixLong(stats?.data?.range_count || 0).toNumber(),
            nodesByRegionString: getNodesByRegionString(nodes, nodeRegions),
          },
        };
      }),
    };
  },
);

export const mapDispatchToProps = {
  refreshDatabaseDetails: (database: string) => {
    return refreshDatabaseDetails(
      new DatabaseDetailsRequest({ database, include_stats: true }),
    );
  },
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
};
