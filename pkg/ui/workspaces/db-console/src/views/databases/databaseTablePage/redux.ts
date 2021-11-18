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
  refreshNodes,
  refreshIndexStats,
} from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { databaseNameAttr, tableNameAttr } from "src/util/constants";
import { FixLong, longToInt } from "src/util/fixLong";
import { getMatchParamByName } from "src/util/query";
import {
  nodeRegionsByIDSelector,
  selectIsMoreThanOneNode,
} from "src/redux/nodes";
import { getNodesByRegionString } from "../utils";
import { TimestampToMoment } from "src/util/convert";
import { formatDate } from "antd/es/date-picker/utils";
import { resetIndexUsageStatsAction } from "oss/src/redux/indexUsageStats";
import moment from "moment";

const {
  TableDetailsRequest,
  TableStatsRequest,
  TableIndexStatsRequest,
} = cockroach.server.serverpb;

export const mapStateToProps = createSelector(
  (_state: AdminUIState, props: RouteComponentProps): string =>
    getMatchParamByName(props.match, databaseNameAttr),
  (_state: AdminUIState, props: RouteComponentProps): string =>
    getMatchParamByName(props.match, tableNameAttr),

  state => state.cachedData.tableDetails,
  state => state.cachedData.tableStats,
  state => state.cachedData.indexStats,
  state => nodeRegionsByIDSelector(state),
  state => selectIsMoreThanOneNode(state),

  (
    database,
    table,
    tableDetails,
    tableStats,
    indexUsageStats,
    nodeRegions,
    showNodeRegionsSection,
  ): DatabaseTablePageData => {
    const details = tableDetails[generateTableID(database, table)];
    const stats = tableStats[generateTableID(database, table)];
    const indexStats = indexUsageStats[generateTableID(database, table)];

    let lastResetString: string;
    const minDate = moment.utc("0001-01-01"); // minimum value as per UTC
    const lastReset = TimestampToMoment(indexStats?.data?.last_reset)
    if (lastReset.isSame(minDate)) {
      lastResetString = "never";
    } else {
      lastResetString = formatDate(lastReset, "MMM DD, YYYY [at] h:mm A [(UTC)]");
    }

    const indexStatsData = _.flatMap(
      indexStats?.data?.statistics,
      indexStat => {
        const lastRead = TimestampToMoment(indexStat.statistics?.stats?.last_read);
        let lastUsed, lastUsedString;
        if (lastRead.isAfter(lastReset)) {
          lastUsed = lastRead;
          lastUsedString = formatDate(
            lastUsed,
            "[Last read:] MMM DD, YYYY [at] h:mm A",
          );
        } else {
          // todo @lindseyjin: replace default with create time after it's added to table_indexes
          lastUsed = lastReset;
          if (lastReset.isSame(minDate)) {
            lastUsedString = "Last reset: never"
          } else {
            lastUsedString = formatDate(
              lastUsed,
              "[Last reset:] MMM DD, YYYY [at] h:mm A",
            );
          }
        }
        return {
          indexName: indexStat.index_name,
          totalReads: longToInt(indexStat.statistics?.stats?.total_read_count),
          lastUsedTime: lastUsed,
          lastUsedString: lastUsedString,
        };
      },
    );
    const grants = _.flatMap(details?.data?.grants, grant =>
      _.map(grant.privileges, privilege => {
        return { user: grant.user, privilege };
      }),
    );
    const nodes = stats?.data?.node_ids || [];

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
      showNodeRegionsSection,
      stats: {
        loading: !!stats?.inFlight,
        loaded: !!stats?.valid,
        sizeInBytes: FixLong(
          stats?.data?.approximate_disk_bytes || 0,
        ).toNumber(),
        rangeCount: FixLong(stats?.data?.range_count || 0).toNumber(),
        nodesByRegionString: getNodesByRegionString(nodes, nodeRegions),
      },
      indexStats: {
        loading: !!indexStats?.inFlight,
        loaded: !!indexStats?.valid,
        stats: indexStatsData,
        lastReset: lastResetString,
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

  refreshIndexStats: (database: string, table: string) => {
    return refreshIndexStats(new TableIndexStatsRequest({ database, table }));
  },

  resetIndexUsageStats: resetIndexUsageStatsAction,

  refreshNodes,
};
