// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { Dispatch } from "redux";

import { AppState, uiConfigActions } from "src/store";
import { actions as statementDiagnosticsActions } from "src/store/statementDiagnostics";
import { actions as analyticsActions } from "src/store/analytics";
import {
  actions as localStorageActions,
  updateStmtsPageLimitAction,
  updateStmsPageReqSortAction,
} from "src/store/localStorage";
import { actions as sqlStatsActions } from "src/store/sqlStats";
import { actions as databasesListActions } from "src/store/databasesList";
import { actions as nodesActions } from "../store/nodes";
import {
  StatementsPageDispatchProps,
  StatementsPageStateProps,
} from "./statementsPage";
import {
  selectDatabases,
  selectLastReset,
  selectStatements,
  selectStatementsDataValid,
  selectStatementsDataInFlight,
  selectStatementsLastError,
  selectColumns,
  selectSortSetting,
  selectFilters,
  selectSearch,
  selectStatementsLastUpdated,
} from "./statementsPage.selectors";
import {
  selectTimeScale,
  selectStmtsPageLimit,
  selectStmtsPageReqSort,
} from "../store/utils/selectors";
import {
  selectIsTenant,
  selectHasViewActivityRedactedRole,
  selectHasAdminRole,
} from "../store/uiConfig";
import { nodeRegionsByIDSelector } from "../store/nodes";
import { StatementsRequest } from "src/api/statementsApi";
import { TimeScale } from "../timeScaleDropdown";
import {
  StatementsPageRoot,
  StatementsPageRootProps,
} from "./statementsPageRoot";
import {
  RecentStatementsViewDispatchProps,
  RecentStatementsViewStateProps,
} from "./recentStatementsView";
import {
  mapDispatchToRecentStatementsPageProps,
  mapStateToRecentStatementsPageProps,
} from "./recentStatementsPage.selectors";
import {
  InsertStmtDiagnosticRequest,
  StatementDiagnosticsReport,
  SqlStatsSortType,
} from "../api";
import { selectStmtsAllApps } from "../selectors";

type StateProps = {
  fingerprintsPageProps: StatementsPageStateProps & RouteComponentProps;
  activePageProps: RecentStatementsViewStateProps;
};

type DispatchProps = {
  fingerprintsPageProps: StatementsPageDispatchProps;
  activePageProps: RecentStatementsViewDispatchProps;
};

export const ConnectedStatementsPage = withRouter(
  connect<
    StateProps,
    DispatchProps,
    RouteComponentProps,
    StatementsPageRootProps
  >(
    (state: AppState, props: RouteComponentProps) => ({
      fingerprintsPageProps: {
        ...props,
        apps: selectStmtsAllApps(state.adminUI?.statements?.data),
        columns: selectColumns(state),
        databases: selectDatabases(state),
        timeScale: selectTimeScale(state),
        filters: selectFilters(state),
        isTenant: selectIsTenant(state),
        hasViewActivityRedactedRole: selectHasViewActivityRedactedRole(state),
        hasAdminRole: selectHasAdminRole(state),
        lastReset: selectLastReset(state),
        nodeRegions: nodeRegionsByIDSelector(state),
        search: selectSearch(state),
        sortSetting: selectSortSetting(state),
        statements: selectStatements(state, props),
        isDataValid: selectStatementsDataValid(state),
        isReqInFlight: selectStatementsDataInFlight(state),
        lastUpdated: selectStatementsLastUpdated(state),
        statementsError: selectStatementsLastError(state),
        limit: selectStmtsPageLimit(state),
        reqSortSetting: selectStmtsPageReqSort(state),
        stmtsTotalRuntimeSecs:
          state.adminUI?.statements?.data?.stmts_total_runtime_secs ?? 0,
      },
      activePageProps: mapStateToRecentStatementsPageProps(state),
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
        onFilterChange: value => {
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
      activePageProps: mapDispatchToRecentStatementsPageProps(dispatch),
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
