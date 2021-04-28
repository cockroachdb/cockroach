// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import { connect } from "react-redux";
import {
  RouteComponentProps,
  match as Match,
  withRouter,
} from "react-router-dom";
import { createSelector } from "reselect";
import _ from "lodash";

import {
  refreshLiveness,
  refreshNodes,
  refreshStatementDiagnosticsRequests,
  refreshStatements,
} from "src/redux/apiReducers";
import {
  nodeDisplayNameByIDSelector,
  nodeRegionsByIDSelector,
} from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import {
  combineStatementStats,
  ExecutionStatistics,
  flattenStatementStats,
  statementKey,
  StatementStatistics,
} from "src/util/appStats";
import {
  appAttr,
  databaseAttr,
  implicitTxnAttr,
  statementAttr,
} from "src/util/constants";
import { FixLong } from "src/util/fixLong";
import { getMatchParamByName } from "src/util/query";
import { selectDiagnosticsReportsByStatementFingerprint } from "src/redux/statements/statementsSelectors";
import {
  StatementDetails,
  StatementDetailsDispatchProps,
  StatementDetailsStateProps,
  StatementDetailsProps,
  AggregateStatistics,
} from "@cockroachlabs/cluster-ui";
import { createStatementDiagnosticsReportAction } from "src/redux/statements";
import { createStatementDiagnosticsAlertLocalSetting } from "src/redux/alerts";
import {
  trackDownloadDiagnosticsBundleAction,
  trackStatementDetailsSubnavSelectionAction,
} from "src/redux/analyticsActions";

interface Fraction {
  numerator: number;
  denominator: number;
}

interface StatementDetailsData {
  nodeId: number;
  implicitTxn: boolean;
  fullScan: boolean;
  database: string;
  stats: StatementStatistics[];
}

function coalesceNodeStats(
  stats: ExecutionStatistics[],
): AggregateStatistics[] {
  const statsKey: { [nodeId: string]: StatementDetailsData } = {};

  stats.forEach((stmt) => {
    const key = statementKey(stmt);
    if (!(key in statsKey)) {
      statsKey[key] = {
        nodeId: stmt.node_id,
        implicitTxn: stmt.implicit_txn,
        fullScan: stmt.full_scan,
        database: stmt.database,
        stats: [],
      };
    }
    statsKey[key].stats.push(stmt.stats);
  });

  return Object.keys(statsKey).map((key) => {
    const stmt = statsKey[key];
    return {
      label: stmt.nodeId.toString(),
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

  stats.forEach((stmt) => {
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
  const database = getMatchParamByName(match, databaseAttr);
  let app = getMatchParamByName(match, appAttr);

  const filterByKeys = (stmt: ExecutionStatistics) =>
    stmt.statement === statement &&
    stmt.implicit_txn === implicitTxn &&
    (stmt.database === database || database === null);

  if (!app) {
    return filterByKeys;
  }

  if (app === "(unset)") {
    app = "";
  }

  if (app === "(internal)") {
    return (stmt: ExecutionStatistics) =>
      filterByKeys(stmt) && stmt.app.startsWith(internalAppNamePrefix);
  }

  return (stmt: ExecutionStatistics) => filterByKeys(stmt) && stmt.app === app;
}

export const selectStatement = createSelector(
  (state: AdminUIState) => state.cachedData.statements,
  (_state: AdminUIState, props: RouteComponentProps) => props,
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
      stats: combineStatementStats(results.map((s) => s.stats)),
      byNode: coalesceNodeStats(results),
      app: _.uniq(results.map((s) => s.app)),
      database: getMatchParamByName(props.match, databaseAttr),
      distSQL: fractionMatching(results, (s) => s.distSQL),
      vec: fractionMatching(results, (s) => s.vec),
      opt: fractionMatching(results, (s) => s.opt),
      implicit_txn: fractionMatching(results, (s) => s.implicit_txn),
      full_scan: fractionMatching(results, (s) => s.full_scan),
      failed: fractionMatching(results, (s) => s.failed),
      node_id: _.uniq(results.map((s) => s.node_id)),
    };
  },
);

const mapStateToProps = (
  state: AdminUIState,
  props: StatementDetailsProps,
): StatementDetailsStateProps => {
  const statement = selectStatement(state, props);
  const statementFingerprint = statement?.statement;
  return {
    statement,
    statementsError: state.cachedData.statements.lastError,
    nodeNames: nodeDisplayNameByIDSelector(state),
    nodeRegions: nodeRegionsByIDSelector(state),
    diagnosticsReports: selectDiagnosticsReportsByStatementFingerprint(
      state,
      statementFingerprint,
    ),
  };
};

const mapDispatchToProps: StatementDetailsDispatchProps = {
  refreshStatements,
  refreshStatementDiagnosticsRequests,
  dismissStatementDiagnosticsAlertMessage: () =>
    createStatementDiagnosticsAlertLocalSetting.set({ show: false }),
  createStatementDiagnosticsReport: createStatementDiagnosticsReportAction,
  onTabChanged: trackStatementDetailsSubnavSelectionAction,
  onDiagnosticBundleDownload: trackDownloadDiagnosticsBundleAction,
  refreshNodes: refreshNodes,
  refreshNodesLiveness: refreshLiveness,
};

export default withRouter(
  connect<StatementDetailsStateProps, StatementDetailsDispatchProps>(
    mapStateToProps,
    mapDispatchToProps,
  )(StatementDetails),
);
