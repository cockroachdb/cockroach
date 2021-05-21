import { createSelector } from "@reduxjs/toolkit";
import { RouteComponentProps, match as Match } from "react-router-dom";
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
  StatementStatistics,
} from "../util";
import { AggregateStatistics } from "../statementsTable";
import { Fraction } from "./statementDetails";

interface StatementDetailsData {
  nodeId: number;
  implicitTxn: boolean;
  stats: StatementStatistics[];
}

function keyByNodeAndImplicitTxn(stmt: ExecutionStatistics): string {
  return stmt.node_id.toString() + stmt.implicit_txn;
}

function coalesceNodeStats(
  stats: ExecutionStatistics[],
): AggregateStatistics[] {
  const byNodeAndImplicitTxn: { [nodeId: string]: StatementDetailsData } = {};

  stats.forEach(stmt => {
    const key = keyByNodeAndImplicitTxn(stmt);
    if (!(key in byNodeAndImplicitTxn)) {
      byNodeAndImplicitTxn[key] = {
        nodeId: stmt.node_id,
        implicitTxn: stmt.implicit_txn,
        stats: [],
      };
    }
    byNodeAndImplicitTxn[key].stats.push(stmt.stats);
  });

  return Object.keys(byNodeAndImplicitTxn).map(key => {
    const stmt = byNodeAndImplicitTxn[key];
    return {
      label: stmt.nodeId.toString(),
      implicitTxn: stmt.implicitTxn,
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
  internalAppNamePrefix: string,
): (stat: ExecutionStatistics) => boolean {
  const statement = getMatchParamByName(match, statementAttr);
  const implicitTxn = getMatchParamByName(match, implicitTxnAttr) === "true";
  let app = getMatchParamByName(match, appAttr);

  const filterByStatementAndImplicitTxn = (stmt: ExecutionStatistics) =>
    stmt.statement === statement && stmt.implicit_txn === implicitTxn;

  if (!app) {
    return filterByStatementAndImplicitTxn;
  }

  if (app === "(unset)") {
    app = "";
  }

  if (app === "(internal)") {
    return (stmt: ExecutionStatistics) =>
      filterByStatementAndImplicitTxn(stmt) &&
      stmt.app.startsWith(internalAppNamePrefix);
  }

  return (stmt: ExecutionStatistics) =>
    filterByStatementAndImplicitTxn(stmt) && stmt.app === app;
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
      filterByRouterParamsPredicate(props.match, internalAppNamePrefix),
    );
    const statement = getMatchParamByName(props.match, statementAttr);
    return {
      statement,
      stats: combineStatementStats(results.map(s => s.stats)),
      byNode: coalesceNodeStats(results),
      app: _.uniq(results.map(s => s.app)),
      distSQL: fractionMatching(results, s => s.distSQL),
      vec: fractionMatching(results, s => s.vec),
      opt: fractionMatching(results, s => s.opt),
      implicit_txn: fractionMatching(results, s => s.implicit_txn),
      failed: fractionMatching(results, s => s.failed),
      node_id: _.uniq(results.map(s => s.node_id)),
    };
  },
);

export const selectStatementDetailsUiConfig = createSelector(
  (state: AppState) => state.adminUI.uiConfig.pages.statementDetails,
  statementDetailsUiConfig => statementDetailsUiConfig,
);
