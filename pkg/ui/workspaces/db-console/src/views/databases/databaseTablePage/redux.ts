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
import _ from "lodash";
import { DatabaseTablePageData } from "@cockroachlabs/cluster-ui";

import { cockroach } from "src/js/protos";
import {
  generateTableID,
  refreshTableDetails,
  refreshTableStats,
} from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { databaseNameAttr, tableNameAttr } from "src/util/constants";
import { FixLong } from "src/util/fixLong";
import { getMatchParamByName } from "src/util/query";

const { TableDetailsRequest, TableStatsRequest } = cockroach.server.serverpb;

export const mapStateToProps = createSelector(
  (_state: AdminUIState, props: RouteComponentProps): string =>
    getMatchParamByName(props.match, databaseNameAttr),
  (_state: AdminUIState, props: RouteComponentProps): string =>
    getMatchParamByName(props.match, tableNameAttr),

  state => state.cachedData.tableDetails,
  state => state.cachedData.tableStats,

  (database, table, tableDetails, tableStats): DatabaseTablePageData => {
    const details = tableDetails[generateTableID(database, table)];
    const stats = tableStats[generateTableID(database, table)];
    const grants = _.flatMap(details?.data?.grants, grant =>
      _.map(grant.privileges, privilege => {
        return { user: grant.user, privilege };
      }),
    );

    return {
      databaseName: database,
      name: table,
      details: {
        loading: !!details?.inFlight,
        loaded: !!details?.valid,
        createStatement: details?.data?.create_table_statement || "",
        replicaCount: details?.data?.zone_config?.num_replicas || 0,
        indexNames: _.uniq(_.map(details?.data?.indexes, index => index.name)),
        grants: grants,
      },
      stats: {
        loading: !!stats?.inFlight,
        loaded: !!stats?.valid,
        sizeInBytes: FixLong(
          stats?.data?.approximate_disk_bytes || 0,
        ).toNumber(),
        rangeCount: FixLong(stats?.data?.range_count || 0).toNumber(),
      },
    };
  },
);

export const mapDispatchToProps = {
  refreshTableDetails: (database: string, table: string) => {
    return refreshTableDetails(new TableDetailsRequest({ database, table }));
  },

  refreshTableStats: (database: string, table: string) => {
    return refreshTableStats(new TableStatsRequest({ database, table }));
  },
};
