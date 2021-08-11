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
import moment, { Moment } from "moment";

import { AppState } from "src/store";
import { actions as statementsActions } from "src/store/statements";
import { actions as statementDiagnosticsActions } from "src/store/statementDiagnostics";
import { actions as analyticsActions } from "src/store/analytics";
import { actions as localStorageActions } from "src/store/localStorage";
import { actions as resetSQLStatsActions } from "src/store/sqlStats";
import {
  StatementsPage,
  StatementsPageDispatchProps,
  StatementsPageProps,
  StatementsPageStateProps,
} from "./statementsPage";
import {
  selectApps,
  selectDatabases,
  selectLastReset,
  selectStatements,
  selectStatementsLastError,
  selectTotalFingerprints,
  selectColumns,
  selectDateRange,
} from "./statementsPage.selectors";
import { selectIsTenant } from "../store/uiConfig";
import { AggregateStatistics } from "../statementsTable";
import { nodeRegionsByIDSelector } from "../store/nodes";
import { StatementsRequest } from "src/api/statementsApi";

export const ConnectedStatementsPage = withRouter(
  connect<
    StatementsPageStateProps,
    StatementsPageDispatchProps,
    RouteComponentProps
  >(
    (state: AppState, props: StatementsPageProps) => ({
      statements: selectStatements(state, props),
      statementsError: selectStatementsLastError(state),
      apps: selectApps(state),
      databases: selectDatabases(state),
      totalFingerprints: selectTotalFingerprints(state),
      lastReset: selectLastReset(state),
      columns: selectColumns(state),
      nodeRegions: nodeRegionsByIDSelector(state),
      isTenant: selectIsTenant(state),
      dateRange: selectIsTenant(state) ? null : selectDateRange(state),
    }),
    (dispatch: Dispatch) => ({
      refreshStatements: (req?: StatementsRequest) =>
        dispatch(statementsActions.refresh(req)),
      onDateRangeChange: (start: Moment, end: Moment) => {
        dispatch(
          statementsActions.updateDateRange({
            start: start.unix(),
            end: end.unix(),
          }),
        );
      },
      refreshStatementDiagnosticsRequests: () =>
        dispatch(statementDiagnosticsActions.refresh()),
      resetSQLStats: () => dispatch(resetSQLStatsActions.request()),
      dismissAlertMessage: () =>
        dispatch(
          localStorageActions.update({
            key: "adminUi/showDiagnosticsModal",
            value: false,
          }),
        ),
      onActivateStatementDiagnostics: (statementFingerprint: string) => {
        dispatch(
          statementDiagnosticsActions.createReport(statementFingerprint),
        );
        dispatch(
          analyticsActions.track({
            name: "Statement Diagnostics Clicked",
            page: "Statements",
            action: "Activated",
          }),
        );
      },
      onDiagnosticsReportDownload: () =>
        dispatch(
          analyticsActions.track({
            name: "Statement Diagnostics Clicked",
            page: "Statements",
            action: "Downloaded",
          }),
        ),
      onSearchComplete: (_results: AggregateStatistics[]) =>
        dispatch(
          analyticsActions.track({
            name: "Keyword Searched",
            page: "Statements",
          }),
        ),
      onFilterChange: value =>
        dispatch(
          analyticsActions.track({
            name: "Filter Clicked",
            page: "Statements",
            filterName: "app",
            value,
          }),
        ),
      onSortingChange: (tableName: string, columnName: string) =>
        dispatch(
          analyticsActions.track({
            name: "Column Sorted",
            page: "Statements",
            tableName,
            columnName,
          }),
        ),
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
    }),
  )(StatementsPage),
);
