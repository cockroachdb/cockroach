// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { createSelector } from "reselect";
import { AppState } from "../store";
import { RouteComponentProps } from "react-router";
import {
  databaseNameAttr,
  generateTableID,
  getMatchParamByName,
  indexNameAttr,
  longToInt,
  tableNameAttr,
  TimestampToMoment,
} from "../util";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { IndexDetailsPageData } from "./indexDetailsPage";
const { RecommendationType } = cockroach.sql.IndexRecommendation;

export const selectIndexDetails = createSelector(
  (_state: AppState, props: RouteComponentProps): string =>
    getMatchParamByName(props.match, databaseNameAttr),
  (_state: AppState, props: RouteComponentProps): string =>
    getMatchParamByName(props.match, tableNameAttr),
  (_state: AppState, props: RouteComponentProps): string =>
    getMatchParamByName(props.match, indexNameAttr),
  (state: AppState) => state.adminUI.indexStats.cachedData,
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
        lastRead: TimestampToMoment(details?.statistics?.stats?.last_read),
        lastReset: TimestampToMoment(stats?.data?.last_reset),
        indexRecommendations,
      },
    };
  },
);
