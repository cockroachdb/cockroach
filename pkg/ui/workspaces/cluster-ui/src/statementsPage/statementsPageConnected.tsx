// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { Dispatch } from "redux";

import { StatementsRequest } from "src/api/statementsApi";
import { AppState, uiConfigActions } from "src/store";
import { actions as analyticsActions } from "src/store/analytics";
import { actions as databasesListActions } from "src/store/databasesList";
import {
  actions as localStorageActions,
  updateStmtsPageLimitAction,
  updateStmsPageReqSortAction,
} from "src/store/localStorage";
import { actions as sqlStatsActions } from "src/store/sqlStats";
import { actions as statementDiagnosticsActions } from "src/store/statementDiagnostics";

import {
  InsertStmtDiagnosticRequest,
  StatementDiagnosticsReport,
  SqlStatsSortType,
} from "../api";
import {
  actions as nodesActions,
  nodeRegionsByIDSelector,
} from "../store/nodes";
import {
  selectIsTenant,
  selectHasViewActivityRedactedRole,
  selectHasAdminRole,
} from "../store/uiConfig";
import {
  selectTimeScale,
  selectStmtsPageLimit,
  selectStmtsPageReqSort,
} from "../store/utils/selectors";
import { TimeScale } from "../timeScaleDropdown";

import {
  mapDispatchToActiveStatementsPageProps,
  mapStateToActiveStatementsPageProps,
} from "./activeStatementsPage.selectors";
import {
  ActiveStatementsViewDispatchProps,
  ActiveStatementsViewStateProps,
} from "./activeStatementsView";
import {
  StatementsPageDispatchProps,
  StatementsPageStateProps,
} from "./statementsPage";
import {
  selectDatabases,
  selectColumns,
  selectSortSetting,
  selectFilters,
  selectSearch,
  selectRequestTime,
} from "./statementsPage.selectors";
import {
  StatementsPageRoot,
  StatementsPageRootProps,
} from "./statementsPageRoot";

type StateProps = {
  fingerprintsPageProps: StatementsPageStateProps & RouteComponentProps;
  activePageProps: ActiveStatementsViewStateProps;
};

type DispatchProps = {
  fingerprintsPageProps: StatementsPageDispatchProps;
  activePageProps: ActiveStatementsViewDispatchProps;
};

export const ConnectedStatementsPage = withRouter(
  connect<
    StateProps,
    DispatchProps,
    RouteComponentProps,
    StatementsPageRootProps,
    AppState
  >(
    (state: AppState, props: RouteComponentProps): StateProps => ({
      fingerprintsPageProps: {
        ...props,
        columns: selectColumns(state),
        databases: selectDatabases(state),
        timeScale: selectTimeScale(state),
        filters: selectFilters(state),
        isTenant: selectIsTenant(state),
        hasViewActivityRedactedRole: selectHasViewActivityRedactedRole(state),
        hasAdminRole: selectHasAdminRole(state),
        nodeRegions: nodeRegionsByIDSelector(state),
        search: selectSearch(state),
        sortSetting: selectSortSetting(state),
        limit: selectStmtsPageLimit(state),
        requestTime: selectRequestTime(state),
        reqSortSetting: selectStmtsPageReqSort(state),
        stmtsTotalRuntimeSecs:
          state.adminUI?.statements?.data?.stmts_total_runtime_secs ?? 0,
        statementsResponse: state.adminUI.statements,
        statementDiagnostics: state.adminUI.statementDiagnostics?.data,
        oldestDataAvailable:
          state.adminUI?.statements?.data?.oldest_aggregated_ts_returned,
      },
      activePageProps: mapStateToActiveStatementsPageProps(state),
    }),
    (dispatch: Dispatch) => ({
      fingerprintsPageProps: {
        refreshDatabases: () => dispatch(databasesListActions.refresh()),
        refreshStatements: (req: StatementsRequest) =>
          dispatch(sqlStatsActions.refresh(req)),
        onTimeScaleChange: (ts: TimeScale) => {
          dispatch(
            sqlStatsActions.updateTimeScale({
              ts: ts,
            }),
          );
        },
        refreshStatementDiagnosticsRequests: () =>
          dispatch(statementDiagnosticsActions.refresh()),
        refreshNodes: () => dispatch(nodesActions.refresh()),
        refreshUserSQLRoles: () =>
          dispatch(uiConfigActions.refreshUserSQLRoles()),
        resetSQLStats: () => dispatch(sqlStatsActions.reset()),
        dismissAlertMessage: () =>
          dispatch(
            localStorageActions.update({
              key: "adminUi/showDiagnosticsModal",
              value: false,
            }),
          ),
        onActivateStatementDiagnostics: (
          insertStmtDiagnosticsRequest: InsertStmtDiagnosticRequest,
        ) => {
          dispatch(
            statementDiagnosticsActions.createReport(
              insertStmtDiagnosticsRequest,
            ),
          );
          dispatch(
            analyticsActions.track({
              name: "Statement Diagnostics Clicked",
              page: "Statements",
              action: "Activated",
            }),
          );
        },
        onSelectDiagnosticsReportDropdownOption: (
          report: StatementDiagnosticsReport,
        ) => {
          if (report.completed) {
            dispatch(
              analyticsActions.track({
                name: "Statement Diagnostics Clicked",
                page: "Statements",
                action: "Downloaded",
              }),
            );
          } else {
            dispatch(
              statementDiagnosticsActions.cancelReport({
                requestId: report.id,
              }),
            );
            dispatch(
              analyticsActions.track({
                name: "Statement Diagnostics Clicked",
                page: "Statements",
                action: "Cancelled",
              }),
            );
          }
        },
        onSearchComplete: (query: string) => {
          dispatch(
            analyticsActions.track({
              name: "Keyword Searched",
              page: "Statements",
            }),
          );
          dispatch(
            localStorageActions.update({
              key: "search/StatementsPage",
              value: query,
            }),
          );
        },
        onFilterChange: (value: any) => {
          dispatch(
            analyticsActions.track({
              name: "Filter Clicked",
              page: "Statements",
              filterName: "filters",
              value: value.toString(),
            }),
          );
          dispatch(
            localStorageActions.update({
              key: "filters/StatementsPage",
              value: value,
            }),
          );
        },
        onSortingChange: (
          tableName: string,
          columnName: string,
          ascending: boolean,
        ) => {
          dispatch(
            analyticsActions.track({
              name: "Column Sorted",
              page: "Statements",
              tableName,
              columnName,
            }),
          );
          dispatch(
            localStorageActions.update({
              key: "sortSetting/StatementsPage",
              value: { columnTitle: columnName, ascending: ascending },
            }),
          );
        },
        onRequestTimeChange: (t: moment.Moment) => {
          dispatch(
            localStorageActions.update({
              key: "requestTime/StatementsPage",
              value: t,
            }),
          );
        },
        onStatementClick: () =>
          dispatch(
            analyticsActions.track({
              name: "Statement Clicked",
              page: "Statements",
            }),
          ),
        // We use `null` when the value was never set and it will show all columns.
        // If the user modifies the selection and no columns are selected,
        // the function will save the value as a blank space, otherwise
        // it gets saved as `null`.
        onColumnsChange: (selectedColumns: string[]) =>
          dispatch(
            localStorageActions.update({
              key: "showColumns/StatementsPage",
              value:
                selectedColumns.length === 0 ? " " : selectedColumns.join(","),
            }),
          ),
        onChangeLimit: (limit: number) =>
          dispatch(updateStmtsPageLimitAction(limit)),
        onChangeReqSort: (sort: SqlStatsSortType) =>
          dispatch(updateStmsPageReqSortAction(sort)),
        onApplySearchCriteria: (ts: TimeScale, limit: number, sort: string) =>
          dispatch(
            analyticsActions.track({
              name: "Apply Search Criteria",
              page: "Statements",
              tsValue: ts.key,
              limitValue: limit,
              sortValue: sort,
            }),
          ),
      },
      activePageProps: mapDispatchToActiveStatementsPageProps(dispatch),
    }),
    (stateProps, dispatchProps): StatementsPageRootProps => ({
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
