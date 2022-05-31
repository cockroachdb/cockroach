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
import { IndexDetailsPageData, util } from "@cockroachlabs/cluster-ui";
import { AdminUIState } from "src/redux/state";
import { RouteComponentProps } from "react-router";
import { getMatchParamByName } from "src/util/query";
import {
  databaseNameAttr,
  tableNameAttr,
  indexNameAttr,
} from "src/util/constants";
import {
  generateTableID,
  refreshIndexStats,
  refreshNodes,
} from "src/redux/apiReducers";
import { resetIndexUsageStatsAction } from "src/redux/indexUsageStats";
import { longToInt } from "src/util/fixLong";
import { cockroach } from "src/js/protos";
import TableIndexStatsRequest = cockroach.server.serverpb.TableIndexStatsRequest;
const { RecommendationType } = cockroach.sql.IndexRecommendation;

export const mapStateToProps = createSelector(
  (_state: AdminUIState, props: RouteComponentProps): string =>
    getMatchParamByName(props.match, databaseNameAttr),
  (_state: AdminUIState, props: RouteComponentProps): string =>
    getMatchParamByName(props.match, tableNameAttr),
  (_state: AdminUIState, props: RouteComponentProps): string =>
    getMatchParamByName(props.match, indexNameAttr),
  state => state.cachedData.indexStats,
  (database, table, index, indexStats): IndexDetailsPageData => {
    const stats = indexStats[generateTableID(database, table)];
    const details = stats?.data?.statistics.filter(
      stat => stat.index_name === index, // index names must be unique for a table
    )[0];
    const filteredIndexRecommendations =
      stats?.data?.index_recommendations.filter(
        indexRec => indexRec.index_id === details.statistics.key.index_id,
      ) || [];
    const indexRecommendations = filteredIndexRecommendations.map(indexRec => {
      return {
        type: RecommendationType[indexRec.type].toString(),
        reason: indexRec.reason,
      };
    });

    return {
      databaseName: database,
      tableName: table,
      indexName: index,
      details: {
        loading: !!stats?.inFlight,
        loaded: !!stats?.valid,
        createStatement: details?.create_statement || "",
        totalReads:
          longToInt(details?.statistics?.stats?.total_read_count) || 0,
        lastRead: util.TimestampToMoment(details?.statistics?.stats?.last_read),
        lastReset: util.TimestampToMoment(stats?.data?.last_reset),
        indexRecommendations,
      },
    };
  },
);

export const mapDispatchToProps = {
  refreshIndexStats: (database: string, table: string) => {
    return refreshIndexStats(new TableIndexStatsRequest({ database, table }));
  },
  resetIndexUsageStats: resetIndexUsageStatsAction,
  refreshNodes,
};
