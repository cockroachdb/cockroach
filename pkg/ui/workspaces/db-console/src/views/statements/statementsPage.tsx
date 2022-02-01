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
  refreshUserSQLRoles,
} from "src/redux/apiReducers";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { AdminUIState, AppDispatch } from "src/redux/state";
import { StatementsResponseMessage } from "src/util/api";
import { appAttr } from "src/util/constants";
import { PrintTime } from "src/views/reports/containers/range/print";
import { selectDiagnosticsReportsPerStatement } from "src/redux/statements/statementsSelectors";
import {
  createStatementDiagnosticsAlertLocalSetting,
  cancelStatementDiagnosticsAlertLocalSetting,
} from "src/redux/alerts";
import { statementsTimeScaleLocalSetting } from "src/redux/statementsTimeScale";
import { selectHasViewActivityRedactedRole } from "src/redux/user";
import { queryByName } from "src/util/query";

import {
  StatementsPage,
  AggregateStatistics,
  Filters,
  defaultFilters,
  util,
} from "@cockroachlabs/cluster-ui";
import {
  cancelStatementDiagnosticsReportAction,
  createOpenDiagnosticsModalAction,
  createStatementDiagnosticsReportAction,
  setCombinedStatementsTimeScaleAction,
} from "src/redux/statements";
import {
  trackCancelDiagnosticsBundleAction,
  trackDownloadDiagnosticsBundleAction,
  trackStatementsPaginationAction,
} from "src/redux/analyticsActions";
import { resetSQLStatsAction } from "src/redux/sqlStats";
import { LocalSetting } from "src/redux/localsettings";
import { nodeRegionsByIDSelector } from "src/redux/nodes";

type ICollectedStatementStatistics = protos.cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;
type IStatementDiagnosticsReport = protos.cockroach.server.serverpb.IStatementDiagnosticsReport;

const {
  aggregateStatementStats,
  combineStatementStats,
  flattenStatementStats,
  statementKey,
} = util;

type ExecutionStatistics = util.ExecutionStatistics;
type StatementStatistics = util.StatementStatistics;

interface StatementsSummaryData {
  statementFingerprintID: string;
  statement: string;
  statementSummary: string;
  aggregatedTs: number;
  aggregationInterval: number;
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
    if (!state.data || state.inFlight) {
      return null;
    }
    let statements = flattenStatementStats(state.data.statements);
    const app = queryByName(props.location, appAttr);
    const isInternal = (statement: ExecutionStatistics) =>
      statement.app.startsWith(state.data.internal_app_name_prefix);

    if (app && app !== "All") {
      const criteria = decodeURIComponent(app).split(",");
      let showInternal = false;
      if (criteria.includes(state.data.internal_app_name_prefix)) {
        showInternal = true;
      }
      if (criteria.includes("(unset)")) {
        criteria.push("");
      }

      statements = statements.filter(
        (statement: ExecutionStatistics) =>
          (showInternal && isInternal(statement)) ||
          criteria.includes(statement.app),
      );
    } else {
      // We don't want to show internal statements by default.
      statements = statements.filter(
        (statement: ExecutionStatistics) => !isInternal(statement),
      );
    }

    const statsByStatementKey: {
      [statement: string]: StatementsSummaryData;
    } = {};
    statements.forEach(stmt => {
      const key = statementKey(stmt);
      if (!(key in statsByStatementKey)) {
        statsByStatementKey[key] = {
          statementFingerprintID: stmt.statement_fingerprint_id?.toString(),
          statement: stmt.statement,
          statementSummary: stmt.statement_summary,
          aggregatedTs: stmt.aggregated_ts,
          aggregationInterval: stmt.aggregation_interval,
          implicitTxn: stmt.implicit_txn,
          fullScan: stmt.full_scan,
          database: stmt.database,
          stats: [],
        };
      }
      statsByStatementKey[key].stats.push(stmt.stats);
    });

    return Object.keys(statsByStatementKey).map(key => {
      const stmt = statsByStatementKey[key];
      return {
        aggregatedFingerprintID: stmt.statementFingerprintID,
        label: stmt.statement,
        summary: stmt.statementSummary,
        aggregatedTs: stmt.aggregatedTs,
        aggregationInterval: stmt.aggregationInterval,
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
    const apps: { [app: string]: boolean } = {};
    state.data.statements.forEach(
      (statement: ICollectedStatementStatistics) => {
        const isNotInternalApp =
          state.data.internal_app_name_prefix &&
          !statement.key.key_data.app.startsWith(
            state.data.internal_app_name_prefix,
          );
        if (
          state.data.internal_app_name_prefix == undefined ||
          isNotInternalApp
        ) {
          if (statement.key.key_data.app) {
            apps[statement.key.key_data.app] = true;
          } else {
            sawBlank = true;
          }
        }
      },
    );
    return [].concat(sawBlank ? ["(unset)"] : []).concat(Object.keys(apps));
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
      new Set(
        state.data.statements.map(s =>
          s.key.key_data.database ? s.key.key_data.database : "(unset)",
        ),
      ),
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
    return PrintTime(util.TimestampToMoment(state.data.last_reset));
  },
);

export const statementColumnsLocalSetting = new LocalSetting(
  "create_statement_columns",
  (state: AdminUIState) => state.localSettings,
  null,
);

export const sortSettingLocalSetting = new LocalSetting(
  "sortSetting/StatementsPage",
  (state: AdminUIState) => state.localSettings,
  { ascending: false, columnTitle: "executionCount" },
);

export const filtersLocalSetting = new LocalSetting(
  "filters/StatementsPage",
  (state: AdminUIState) => state.localSettings,
  defaultFilters,
);

export const searchLocalSetting = new LocalSetting(
  "search/StatementsPage",
  (state: AdminUIState) => state.localSettings,
  null,
);

export default withRouter(
  connect(
    (state: AdminUIState, props: RouteComponentProps) => ({
      apps: selectApps(state),
      columns: statementColumnsLocalSetting.selectorToArray(state),
      databases: selectDatabases(state),
      timeScale: statementsTimeScaleLocalSetting.selector(state),
      filters: filtersLocalSetting.selector(state),
      lastReset: selectLastReset(state),
      nodeRegions: nodeRegionsByIDSelector(state),
      search: searchLocalSetting.selector(state),
      sortSetting: sortSettingLocalSetting.selector(state),
      statements: selectStatements(state, props),
      statementsError: state.cachedData.statements.lastError,
      totalFingerprints: selectTotalFingerprints(state),
      hasViewActivityRedactedRole: selectHasViewActivityRedactedRole(state),
    }),
    {
      refreshStatements: refreshStatements,
      onTimeScaleChange: setCombinedStatementsTimeScaleAction,
      refreshStatementDiagnosticsRequests,
      refreshUserSQLRoles,
      resetSQLStats: resetSQLStatsAction,
      dismissAlertMessage: () => {
        return (dispatch: AppDispatch) => {
          dispatch(
            createStatementDiagnosticsAlertLocalSetting.set({ show: false }),
          );
          dispatch(
            cancelStatementDiagnosticsAlertLocalSetting.set({ show: false }),
          );
        };
      },
      onActivateStatementDiagnostics: createStatementDiagnosticsReportAction,
      onDiagnosticsModalOpen: createOpenDiagnosticsModalAction,
      onSearchComplete: (query: string) => searchLocalSetting.set(query),
      onPageChanged: trackStatementsPaginationAction,
      onSortingChange: (
        _tableName: string,
        columnName: string,
        ascending: boolean,
      ) =>
        sortSettingLocalSetting.set({
          ascending: ascending,
          columnTitle: columnName,
        }),
      onFilterChange: (filters: Filters) => filtersLocalSetting.set(filters),
      onSelectDiagnosticsReportDropdownOption: (
        report: IStatementDiagnosticsReport,
      ) => {
        if (report.completed) {
          return trackDownloadDiagnosticsBundleAction(
            report.statement_fingerprint,
          );
        } else {
          return (dispatch: AppDispatch) => {
            dispatch(cancelStatementDiagnosticsReportAction(report.id));
            dispatch(
              trackCancelDiagnosticsBundleAction(report.statement_fingerprint),
            );
          };
        }
      },
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
