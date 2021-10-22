// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { createSelector } from "@reduxjs/toolkit";
import { RouteComponentProps, match as Match } from "react-router-dom";
import { Location } from "history";
import _ from "lodash";
import { AppState } from "../store";
import {
  appAttr,
  combineStatementStats,
  ExecutionStatistics,
  FixLong,
  flattenStatementStats,
  getMatchParamByName,
  implicitTxnAttr,
  statementAttr,
  databaseAttr,
  StatementStatistics,
  statementKey,
  aggregatedTsAttr,
  queryByName,
} from "../util";
import { AggregateStatistics } from "../statementsTable";
import { Fraction } from "./statementDetails";

interface StatementDetailsData {
  nodeId: number;
  aggregatedTs: number;
  implicitTxn: boolean;
  fullScan: boolean;
  database: string;
  stats: StatementStatistics[];
}

function coalesceNodeStats(
  stats: ExecutionStatistics[],
): AggregateStatistics[] {
  const statsKey: { [nodeId: string]: StatementDetailsData } = {};

  stats.forEach(stmt => {
    const key = statementKey(stmt);
    if (!(key in statsKey)) {
      statsKey[key] = {
        nodeId: stmt.node_id,
        aggregatedTs: stmt.aggregated_ts,
        implicitTxn: stmt.implicit_txn,
        fullScan: stmt.full_scan,
        database: stmt.database,
        stats: [],
      };
    }
    statsKey[key].stats.push(stmt.stats);
  });

  return Object.keys(statsKey).map(key => {
    const stmt = statsKey[key];
    return {
      label: stmt.nodeId.toString(),
      aggregatedTs: stmt.aggregatedTs,
      implicitTxn: stmt.implicitTxn,
      fullScan: stmt.fullScan,
      database: stmt.database,
      stats: combineStatementStats(stmt.stats),
    };
  });
}

function fractionMatching(
  stats: ExecutionStatistics[],
  predicate: (stmt: ExecutionStatistics) => boolean,
): Fraction {
  let numerator = 0;
  let denominator = 0;

  stats.forEach(stmt => {
    const count = FixLong(stmt.stats.first_attempt_count).toInt();
    denominator += count;
    if (predicate(stmt)) {
      numerator += count;
    }
  });

  return { numerator, denominator };
}

function filterByRouterParamsPredicate(
  match: Match<any>,
  location: Location,
  internalAppNamePrefix: string,
): (stat: ExecutionStatistics) => boolean {
  const statement = getMatchParamByName(match, statementAttr);
  const implicitTxn = getMatchParamByName(match, implicitTxnAttr) === "true";
  const database =
    queryByName(location, databaseAttr) === "(unset)"
      ? ""
      : queryByName(location, databaseAttr);
  const apps = queryByName(location, appAttr)
    ? queryByName(location, appAttr).split(",")
    : null;
  // If the aggregatedTs is unset, we will aggregate across the current date range.
  const aggregatedTs = queryByName(location, aggregatedTsAttr);

  const filterByKeys = (stmt: ExecutionStatistics) =>
    stmt.statement === statement &&
    (aggregatedTs == null || stmt.aggregated_ts.toString() === aggregatedTs) &&
    stmt.implicit_txn === implicitTxn &&
    (stmt.database === database || database === null);

  if (!apps) {
    return filterByKeys;
  }
  if (apps.includes("(unset)")) {
    apps.push("");
  }
  let showInternal = false;
  if (apps.includes(internalAppNamePrefix)) {
    showInternal = true;
  }

  return (stmt: ExecutionStatistics) =>
    filterByKeys(stmt) &&
    ((showInternal && stmt.app.startsWith(internalAppNamePrefix)) ||
      apps.includes(stmt.app));
}

export const selectStatement = createSelector(
  (state: AppState) => state.adminUI.statements,
  (_state: AppState, props: RouteComponentProps) => props,
  (statementsState, props) => {
    const statements = statementsState.data?.statements;
    if (!statements) {
      return null;
    }

    const internalAppNamePrefix =
      statementsState.data?.internal_app_name_prefix;
    const flattened = flattenStatementStats(statements);
    const results = _.filter(
      flattened,
      filterByRouterParamsPredicate(
        props.match,
        props.location,
        internalAppNamePrefix,
      ),
    );
    const statement = getMatchParamByName(props.match, statementAttr);
    return {
      statement,
      stats: combineStatementStats(results.map(s => s.stats)),
      byNode: coalesceNodeStats(results),
      app: _.uniq(
        results.map(s =>
          s.app.startsWith(internalAppNamePrefix)
            ? internalAppNamePrefix
            : s.app,
        ),
      ),
      database: queryByName(props.location, databaseAttr),
      distSQL: fractionMatching(results, s => s.distSQL),
      vec: fractionMatching(results, s => s.vec),
      opt: fractionMatching(results, s => s.opt),
      implicit_txn: fractionMatching(results, s => s.implicit_txn),
      full_scan: fractionMatching(results, s => s.full_scan),
      failed: fractionMatching(results, s => s.failed),
      node_id: _.uniq(results.map(s => s.node_id)),
    };
  },
);

export const selectStatementDetailsUiConfig = createSelector(
  (state: AppState) => state.adminUI.uiConfig.pages.statementDetails,
  statementDetailsUiConfig => statementDetailsUiConfig,
);
