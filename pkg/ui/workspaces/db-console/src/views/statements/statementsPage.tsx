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
import { bindActionCreators } from "redux";
import { createSelector } from "reselect";
import { RouteComponentProps, withRouter } from "react-router-dom";
import {
  refreshNodes,
  refreshDatabases,
  refreshStatementDiagnosticsRequests,
  refreshStatements,
  refreshUserSQLRoles,
} from "src/redux/apiReducers";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { AdminUIState, AppDispatch } from "src/redux/state";
import { StatementsResponseMessage } from "src/util/api";
import { PrintTime } from "src/views/reports/containers/range/print";
import {
  createStatementDiagnosticsAlertLocalSetting,
  cancelStatementDiagnosticsAlertLocalSetting,
} from "src/redux/alerts";
import {
  selectHasViewActivityRedactedRole,
  selectHasAdminRole,
} from "src/redux/user";

import {
  Filters,
  defaultFilters,
  util,
  StatementsPageRoot,
  ActiveStatementsViewStateProps,
  StatementsPageStateProps,
  ActiveStatementsViewDispatchProps,
  StatementsPageDispatchProps,
  StatementsPageRootProps,
  api,
} from "@cockroachlabs/cluster-ui";
import {
  cancelStatementDiagnosticsReportAction,
  createOpenDiagnosticsModalAction,
  createStatementDiagnosticsReportAction,
  setGlobalTimeScaleAction,
} from "src/redux/statements";
import {
  trackApplySearchCriteriaAction,
  trackCancelDiagnosticsBundleAction,
  trackDownloadDiagnosticsBundleAction,
  trackStatementsPaginationAction,
} from "src/redux/analyticsActions";
import { resetSQLStatsAction } from "src/redux/sqlStats";
import { LocalSetting } from "src/redux/localsettings";
import { nodeRegionsByIDSelector } from "src/redux/nodes";
import {
  activeStatementsViewActions,
  mapStateToActiveStatementViewProps,
} from "./activeStatementsSelectors";
import { selectTimeScale } from "src/redux/timeScale";
import { createSelectorForCachedDataField } from "src/redux/apiReducers";

// selectDatabases returns the array of all databases in the cluster.
export const selectDatabases = createSelector(
  (state: AdminUIState) => state.cachedData.databases,
  (state: CachedDataReducerState<api.DatabasesListResponse>) => {
    if (!state?.data) {
      return [];
    }

    return state.data.databases
      .filter((dbName: string) => dbName !== null && dbName.length > 0)
      .sort();
  },
);

// selectLastReset returns a string displaying the last time the statement
// statistics were reset.
export const selectLastReset = createSelector(
  (state: AdminUIState) => state.cachedData.statements,
  (state: CachedDataReducerState<StatementsResponseMessage>) => {
    if (!state?.data) {
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
  "tableSortSetting/StatementsPage",
  (state: AdminUIState) => state.localSettings,
  { ascending: false, columnTitle: "executionCount" },
);

export const requestTimeLocalSetting = new LocalSetting(
  "requestTime/StatementsPage",
  (state: AdminUIState) => state.localSettings,
  null,
);

export const filtersLocalSetting = new LocalSetting<AdminUIState, Filters>(
  "filters/StatementsPage",
  (state: AdminUIState) => state.localSettings,
  defaultFilters,
);

export const searchLocalSetting = new LocalSetting(
  "search/StatementsPage",
  (state: AdminUIState) => state.localSettings,
  null,
);

export const reqSortSetting = new LocalSetting(
  "reqSortSetting/StatementsPage",
  (state: AdminUIState) => state.localSettings,
  api.DEFAULT_STATS_REQ_OPTIONS.sortStmt,
);

export const limitSetting = new LocalSetting(
  "reqLimitSetting/StatementsPage",
  (state: AdminUIState) => state.localSettings,
  api.DEFAULT_STATS_REQ_OPTIONS.limit,
);

const fingerprintsPageActions = {
  refreshStatements: refreshStatements,
  refreshDatabases: refreshDatabases,
  onTimeScaleChange: setGlobalTimeScaleAction,
  refreshStatementDiagnosticsRequests,
  refreshNodes,
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
  onActivateStatementDiagnostics: (
    insertStmtDiagnosticRequest: api.InsertStmtDiagnosticRequest,
  ) => {
    return (dispatch: AppDispatch) =>
      dispatch(
        createStatementDiagnosticsReportAction(insertStmtDiagnosticRequest),
      );
  },
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
  onRequestTimeChange: (t: moment.Moment) => requestTimeLocalSetting.set(t),
  onFilterChange: (filters: Filters) => filtersLocalSetting.set(filters),
  onSelectDiagnosticsReportDropdownOption: (
    report: api.StatementDiagnosticsReport,
  ) => {
    if (report.completed) {
      return trackDownloadDiagnosticsBundleAction(report.statement_fingerprint);
    } else {
      return (dispatch: AppDispatch) => {
        dispatch(
          cancelStatementDiagnosticsReportAction({ requestId: report.id }),
        );
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
  onChangeLimit: (newLimit: number) => limitSetting.set(newLimit),
  onChangeReqSort: (sort: api.SqlStatsSortType) => reqSortSetting.set(sort),
  onApplySearchCriteria: trackApplySearchCriteriaAction,
};

type StateProps = {
  fingerprintsPageProps: StatementsPageStateProps;
  activePageProps: ActiveStatementsViewStateProps;
};

type DispatchProps = {
  fingerprintsPageProps: StatementsPageDispatchProps;
  activePageProps: ActiveStatementsViewDispatchProps;
};

const selectStatements =
  createSelectorForCachedDataField<api.SqlStatsResponse>("statements");

const selectStatementDiagnostics =
  createSelectorForCachedDataField<api.StatementDiagnosticsResponse>(
    "statementDiagnosticsReports",
  );

export default withRouter(
  connect<
    StateProps,
    DispatchProps,
    RouteComponentProps,
    StatementsPageRootProps
  >(
    (state: AdminUIState, props: RouteComponentProps) => ({
      fingerprintsPageProps: {
        ...props,
        columns: statementColumnsLocalSetting.selectorToArray(state),
        databases: selectDatabases(state),
        timeScale: selectTimeScale(state),
        filters: filtersLocalSetting.selector(state),
        nodeRegions: nodeRegionsByIDSelector(state),
        search: searchLocalSetting.selector(state),
        sortSetting: sortSettingLocalSetting.selector(state),
        requestTime: requestTimeLocalSetting.selector(state),
        hasViewActivityRedactedRole: selectHasViewActivityRedactedRole(state),
        hasAdminRole: selectHasAdminRole(state),
        limit: limitSetting.selector(state),
        reqSortSetting: reqSortSetting.selector(state),
        stmtsTotalRuntimeSecs:
          state.cachedData?.statements?.data?.stmts_total_runtime_secs ?? 0,
        statementsResponse: selectStatements(state),
        statementDiagnostics: selectStatementDiagnostics(state)?.data,
      },
      activePageProps: mapStateToActiveStatementViewProps(state),
    }),
    (dispatch: AppDispatch): DispatchProps => ({
      fingerprintsPageProps: bindActionCreators(
        fingerprintsPageActions,
        dispatch,
      ),
      activePageProps: bindActionCreators(
        activeStatementsViewActions,
        dispatch,
      ),
    }),
    (stateProps, dispatchProps) => ({
      fingerprintsPageProps: {
        ...stateProps.fingerprintsPageProps,
        ...dispatchProps.fingerprintsPageProps,
      },
      activePageProps: {
        ...stateProps.activePageProps,
        ...dispatchProps.activePageProps,
      },
    }),
  )(StatementsPageRoot),
);
