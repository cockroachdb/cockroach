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
  schemaNameAttr,
  tableNameAttr,
  TimestampToMoment,
} from "../util";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { IndexDetailsPageData } from "./indexDetailsPage";
import {
  selectHasViewActivityRedactedRole,
  selectHasAdminRole,
  selectIsTenant,
} from "../store/uiConfig";
import { BreadcrumbItem } from "../breadcrumbs";
import { RecommendationType as RecType } from "./indexDetailsPage";
import { nodeRegionsByIDSelector } from "../store/nodes";
import { selectTimeScale } from "src/store/utils/selectors";
const { RecommendationType } = cockroach.sql.IndexRecommendation;

export const selectIndexDetails = createSelector(
  (_state: AppState, props: RouteComponentProps): string =>
    getMatchParamByName(props.match, databaseNameAttr),
  (_state: AppState, props: RouteComponentProps): string =>
    getMatchParamByName(props.match, schemaNameAttr),
  (_state: AppState, props: RouteComponentProps): string =>
    getMatchParamByName(props.match, tableNameAttr),
  (_state: AppState, props: RouteComponentProps): string =>
    getMatchParamByName(props.match, indexNameAttr),
  (state: AppState) => state.adminUI?.indexStats.cachedData,
  (state: AppState) => selectIsTenant(state),
  (state: AppState) => selectHasViewActivityRedactedRole(state),
  (state: AppState) => nodeRegionsByIDSelector(state),
  (state: AppState) => selectHasAdminRole(state),
  (state: AppState) => selectTimeScale(state),
  (
    database,
    schema,
    table,
    index,
    indexStats,
    isTenant,
    hasViewActivityRedactedRole,
    nodeRegions,
    hasAdminRole,
    timeScale,
  ): IndexDetailsPageData => {
    const stats = indexStats[generateTableID(database, table)];
    const details = stats?.data?.statistics.filter(
      stat => stat.index_name === index, // index names must be unique for a table
    )[0];
    const filteredIndexRecommendations =
      stats?.data?.index_recommendations.filter(
        indexRec => indexRec.index_id === details?.statistics.key.index_id,
      ) || [];
    const indexRecommendations = filteredIndexRecommendations.map(indexRec => {
      let type: RecType = "Unknown";
      switch (RecommendationType[indexRec.type].toString()) {
        case "DROP_UNUSED":
          type = "DROP_UNUSED";
      }
      return {
        type: type,
        reason: indexRec.reason,
      };
    });

    return {
      databaseName: database,
      tableName: table,
      indexName: index,
      breadcrumbItems: createManagedServiceBreadcrumbs(
        database,
        schema,
        table,
        index,
      ),
      isTenant: isTenant,
      timeScale: timeScale,
      hasViewActivityRedactedRole: hasViewActivityRedactedRole,
      hasAdminRole: hasAdminRole,
      nodeRegions: nodeRegions,
      details: {
        loading: !!stats?.inFlight,
        loaded: !!stats?.valid,
        createStatement: details?.create_statement || "",
        tableID: details?.statistics.key.table_id.toString(),
        indexID: details?.statistics.key.index_id.toString(),
        totalReads:
          longToInt(details?.statistics?.stats?.total_read_count) || 0,
        lastRead: TimestampToMoment(details?.statistics?.stats?.last_read),
        lastReset: TimestampToMoment(stats?.data?.last_reset),
        indexRecommendations,
      },
    };
  },
);

// Note: if the managed-service routes to the index detail or the previous
// database pages change, the breadcrumbs displayed here need to be updated.
function createManagedServiceBreadcrumbs(
  database: string,
  schema: string,
  table: string,
  index: string,
): BreadcrumbItem[] {
  return [
    { link: "/databases", name: "Databases" },
    {
      link: `/databases/${database}`,
      name: "Tables",
    },
    {
      link: `/databases/${database}/${schema}/${table}`,
      name: `Table: ${table}`,
    },
    {
      link: `/databases/${database}/${schema}/${table}/${index}`,
      name: `Index: ${index}`,
    },
  ];
}
