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
import { Location } from "history";
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
  aggregatedTsAttr,
  appAttr,
  databaseAttr,
  implicitTxnAttr,
  statementAttr,
} from "src/util/constants";
import { FixLong } from "src/util/fixLong";
import { getMatchParamByName, queryByName } from "src/util/query";
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
import { selectDateRange } from "src/views/statements/statementsPage";

interface Fraction {
  numerator: number;
  denominator: number;
}

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
    const results = flattened.filter(
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

const mapStateToProps = (
  state: AdminUIState,
  props: StatementDetailsProps,
): StatementDetailsStateProps => {
  const statement = selectStatement(state, props);
  const statementFingerprint = statement?.statement;
  return {
    statement,
    statementsError: state.cachedData.statements.lastError,
    dateRange: selectDateRange(state),
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
