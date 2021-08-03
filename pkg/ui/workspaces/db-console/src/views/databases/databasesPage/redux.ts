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
import { DatabasesPageData } from "@cockroachlabs/cluster-ui";

import { cockroach } from "src/js/protos";
import {
  generateTableID,
  refreshDatabases,
  refreshDatabaseDetails,
  refreshTableStats,
} from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { FixLong } from "src/util/fixLong";

const { DatabaseDetailsRequest, TableStatsRequest } = cockroach.server.serverpb;

export const mapStateToProps = createSelector(
  (state: AdminUIState) => state.cachedData.databases,
  (state: AdminUIState) => state.cachedData.databaseDetails,
  (state: AdminUIState) => state.cachedData.tableStats,

  (databases, databaseDetails, tableStats): DatabasesPageData => {
    return {
      loading: databases.inFlight,
      loaded: databases.valid,
      databases: _.map(databases.data?.databases, database => {
        const details = databaseDetails[database];

        const stats = details?.data?.stats;
        let sizeInBytes = FixLong(
          stats?.approximate_disk_bytes || 0,
        ).toNumber();
        let rangeCount = FixLong(stats?.range_count || 0).toNumber();

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
          ? _.map(stats.missing_tables, table => table.name)
          : details?.data?.table_names;

        const [individuallyLoadedTables, missingTables] = _.partition(
          possiblyMissingTables,
          table => {
            return !!tableStats[generateTableID(database, table)]?.valid;
          },
        );

        _.each(individuallyLoadedTables, table => {
          const stats = tableStats[generateTableID(database, table)];
          sizeInBytes += FixLong(
            stats?.data?.approximate_disk_bytes || 0,
          ).toNumber();
          rangeCount += FixLong(stats?.data?.range_count || 0).toNumber();
        });

        return {
          loading: !!details?.inFlight,
          loaded: !!details?.valid,
          name: database,
          sizeInBytes: sizeInBytes,
          tableCount: details?.data?.table_names?.length || 0,
          rangeCount: rangeCount,
          missingTables: _.map(missingTables, table => {
            return {
              loading: !!tableStats[generateTableID(database, table)]?.inFlight,
              name: table,
            };
          }),
        };
      }),
    };
  },
);

export const mapDispatchToProps = {
  refreshDatabases,

  refreshDatabaseDetails: (database: string) => {
    return refreshDatabaseDetails(
      new DatabaseDetailsRequest({ database, include_stats: true }),
    );
  },

  refreshTableStats: (database: string, table: string) => {
    return refreshTableStats(new TableStatsRequest({ database, table }));
  },
};
