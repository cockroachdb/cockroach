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
import { createSelector } from "reselect";
import { RouteComponentProps, withRouter } from "react-router-dom";
import * as protos from "src/js/protos";
import {
  refreshStatementDiagnosticsRequests,
  refreshStatements,
} from "src/redux/apiReducers";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { AdminUIState } from "src/redux/state";
import { StatementsResponseMessage } from "src/util/api";
import {
  aggregateStatementStats,
  combineStatementStats,
  ExecutionStatistics,
  flattenStatementStats,
  StatementStatistics,
} from "src/util/appStats";
import { appAttr } from "src/util/constants";
import { TimestampToMoment } from "src/util/convert";
import { PrintTime } from "src/views/reports/containers/range/print";
import { selectDiagnosticsReportsPerStatement } from "src/redux/statements/statementsSelectors";
import { createStatementDiagnosticsAlertLocalSetting } from "src/redux/alerts";
import { getMatchParamByName } from "src/util/query";

import {
  StatementsPage,
  AggregateStatistics,
} from "@cockroachlabs/admin-ui-components";
import {
  createOpenDiagnosticsModalAction,
  createStatementDiagnosticsReportAction,
} from "src/redux/statements";
import {
  trackDownloadDiagnosticsBundleAction,
  trackStatementsPaginationAction,
  trackStatementsSearchAction,
  trackTableSortAction,
} from "src/redux/analyticsActions";

type ICollectedStatementStatistics = protos.cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;
type IStatementDiagnosticsReport = protos.cockroach.server.serverpb.IStatementDiagnosticsReport;

interface StatementsSummaryData {
  statement: string;
  implicitTxn: boolean;
  stats: StatementStatistics[];
}

function keyByStatementAndImplicitTxn(stmt: ExecutionStatistics): string {
  return stmt.statement + stmt.implicit_txn;
}

// selectStatements returns the array of AggregateStatistics to show on the
// StatementsPage, based on if the appAttr route parameter is set.
export const selectStatements = createSelector(
  (state: AdminUIState) => state.cachedData.statements,
  (_state: AdminUIState, props: RouteComponentProps) => props,
  selectDiagnosticsReportsPerStatement,
  (
    state: CachedDataReducerState<StatementsResponseMessage>,
    props: RouteComponentProps<any>,
    diagnosticsReportsPerStatement,
  ) => {
    if (!state.data) {
      return null;
    }
    let statements = flattenStatementStats(state.data.statements);
    const app = getMatchParamByName(props.match, appAttr);
    const isInternal = (statement: ExecutionStatistics) =>
      statement.app.startsWith(state.data.internal_app_name_prefix);

    if (app) {
      let criteria = decodeURIComponent(app);
      let showInternal = false;
      if (criteria === "(unset)") {
        criteria = "";
      } else if (criteria === "(internal)") {
        showInternal = true;
      }

      statements = statements.filter(
        (statement: ExecutionStatistics) =>
          (showInternal && isInternal(statement)) || statement.app === criteria,
      );
    } else {
      statements = statements.filter(
        (statement: ExecutionStatistics) => !isInternal(statement),
      );
    }

    const statsByStatementAndImplicitTxn: {
      [statement: string]: StatementsSummaryData;
    } = {};
    statements.forEach((stmt) => {
      const key = keyByStatementAndImplicitTxn(stmt);
      if (!(key in statsByStatementAndImplicitTxn)) {
        statsByStatementAndImplicitTxn[key] = {
          statement: stmt.statement,
          implicitTxn: stmt.implicit_txn,
          stats: [],
        };
      }
      statsByStatementAndImplicitTxn[key].stats.push(stmt.stats);
    });

    return Object.keys(statsByStatementAndImplicitTxn).map((key) => {
      const stmt = statsByStatementAndImplicitTxn[key];
      return {
        label: stmt.statement,
        implicitTxn: stmt.implicitTxn,
        stats: combineStatementStats(stmt.stats),
        diagnosticsReports: diagnosticsReportsPerStatement[stmt.statement],
      };
    });
  },
);

// selectApps returns the array of all apps with statement statistics present
// in the data.
export const selectApps = createSelector(
  (state: AdminUIState) => state.cachedData.statements,
  (state: CachedDataReducerState<StatementsResponseMessage>) => {
    if (!state.data) {
      return [];
    }

    let sawBlank = false;
    let sawInternal = false;
    const apps: { [app: string]: boolean } = {};
    state.data.statements.forEach(
      (statement: ICollectedStatementStatistics) => {
        if (
          state.data.internal_app_name_prefix &&
          statement.key.key_data.app.startsWith(
            state.data.internal_app_name_prefix,
          )
        ) {
          sawInternal = true;
        } else if (statement.key.key_data.app) {
          apps[statement.key.key_data.app] = true;
        } else {
          sawBlank = true;
        }
      },
    );
    return []
      .concat(sawInternal ? ["(internal)"] : [])
      .concat(sawBlank ? ["(unset)"] : [])
      .concat(Object.keys(apps));
  },
);

// selectTotalFingerprints returns the count of distinct statement fingerprints
// present in the data.
export const selectTotalFingerprints = createSelector(
  (state: AdminUIState) => state.cachedData.statements,
  (state: CachedDataReducerState<StatementsResponseMessage>) => {
    if (!state.data) {
      return 0;
    }
    const aggregated = aggregateStatementStats(state.data.statements);
    return aggregated.length;
  },
);

// selectLastReset returns a string displaying the last time the statement
// statistics were reset.
export const selectLastReset = createSelector(
  (state: AdminUIState) => state.cachedData.statements,
  (state: CachedDataReducerState<StatementsResponseMessage>) => {
    if (!state.data) {
      return "unknown";
    }

    return PrintTime(TimestampToMoment(state.data.last_reset));
  },
);

export default withRouter(
  connect(
    (state: AdminUIState, props: RouteComponentProps) => ({
      statements: selectStatements(state, props),
      statementsError: state.cachedData.statements.lastError,
      apps: selectApps(state),
      totalFingerprints: selectTotalFingerprints(state),
      lastReset: selectLastReset(state),
    }),
    {
      refreshStatements,
      refreshStatementDiagnosticsRequests,
      dismissAlertMessage: () =>
        createStatementDiagnosticsAlertLocalSetting.set({ show: false }),
      onActivateStatementDiagnostics: createStatementDiagnosticsReportAction,
      onDiagnosticsModalOpen: createOpenDiagnosticsModalAction,
      onSearchComplete: (results: AggregateStatistics[]) =>
        trackStatementsSearchAction(results.length),
      onPageChanged: trackStatementsPaginationAction,
      onSortingChange: trackTableSortAction,
      onDiagnosticsReportDownload: (report: IStatementDiagnosticsReport) =>
        trackDownloadDiagnosticsBundleAction(report.statement_fingerprint),
    },
  )(StatementsPage),
);
