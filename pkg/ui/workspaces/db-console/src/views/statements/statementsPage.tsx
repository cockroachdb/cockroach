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
  statementKey,
  StatementStatistics,
} from "src/util/appStats";
import { appAttr } from "src/util/constants";
import { TimestampToMoment } from "src/util/convert";
import { PrintTime } from "src/views/reports/containers/range/print";
import { selectDiagnosticsReportsPerStatement } from "src/redux/statements/statementsSelectors";
import { createStatementDiagnosticsAlertLocalSetting } from "src/redux/alerts";
import { getMatchParamByName } from "src/util/query";

import { StatementsPage, AggregateStatistics } from "@cockroachlabs/cluster-ui";
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
import { resetSQLStatsAction } from "src/redux/sqlStats";
import { LocalSetting } from "src/redux/localsettings";
import { nodeRegionsByIDSelector } from "src/redux/nodes";

type ICollectedStatementStatistics = protos.cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;
type IStatementDiagnosticsReport = protos.cockroach.server.serverpb.IStatementDiagnosticsReport;

interface StatementsSummaryData {
  statement: string;
  implicitTxn: boolean;
  fullScan: boolean;
  database: string;
  stats: StatementStatistics[];
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
  ): AggregateStatistics[] => {
    if (!state.data) {
      return null;
    }
    let statements = flattenStatementStats(state.data.statements);
    const app = getMatchParamByName(props.match, appAttr);
    const isInternal = (statement: ExecutionStatistics) =>
      statement.app.startsWith(state.data.internal_app_name_prefix);

    if (app && app !== "All") {
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
    }

    const statsByStatementKey: {
      [statement: string]: StatementsSummaryData;
    } = {};
    statements.forEach((stmt) => {
      const key = statementKey(stmt);
      if (!(key in statsByStatementKey)) {
        statsByStatementKey[key] = {
          statement: stmt.statement,
          implicitTxn: stmt.implicit_txn,
          fullScan: stmt.full_scan,
          database: stmt.database,
          stats: [],
        };
      }
      statsByStatementKey[key].stats.push(stmt.stats);
    });

    return Object.keys(statsByStatementKey).map((key) => {
      const stmt = statsByStatementKey[key];
      return {
        label: stmt.statement,
        implicitTxn: stmt.implicitTxn,
        fullScan: stmt.fullScan,
        database: stmt.database,
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

// selectDatabases returns the array of all databases with statement statistics present
// in the data.
export const selectDatabases = createSelector(
  (state: AdminUIState) => state.cachedData.statements,
  (state: CachedDataReducerState<StatementsResponseMessage>) => {
    if (!state.data) {
      return [];
    }
    return Array.from(
      new Set(state.data.statements.map((s) => s.key.key_data.database)),
    ).filter((dbName: string) => dbName !== null && dbName.length > 0);
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

export const statementColumnsLocalSetting = new LocalSetting(
  "create_statement_columns",
  (state: AdminUIState) => state.localSettings,
  null,
);

export default withRouter(
  connect(
    (state: AdminUIState, props: RouteComponentProps) => ({
      statements: selectStatements(state, props),
      statementsError: state.cachedData.statements.lastError,
      apps: selectApps(state),
      databases: selectDatabases(state),
      totalFingerprints: selectTotalFingerprints(state),
      lastReset: selectLastReset(state),
      columns: statementColumnsLocalSetting.selectorToArray(state),
      nodeRegions: nodeRegionsByIDSelector(state),
    }),
    {
      refreshStatements,
      refreshStatementDiagnosticsRequests,
      resetSQLStats: resetSQLStatsAction,
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
      // We use `null` when the value was never set and it will show all columns.
      // If the user modifies the selection and no columns are selected,
      // the function will save the value as a blank space, otherwise
      // it gets saved as `null`.
      onColumnsChange: (value: string[]) =>
        statementColumnsLocalSetting.set(
          value.length === 0 ? " " : value.join(","),
        ),
    },
  )(StatementsPage),
);
