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

import { AppState } from "src/store";
import { actions as statementActions } from "src/store/statements";
import { actions as statementDiagnosticsActions } from "src/store/statementDiagnostics";
import { actions as analyticsActions } from "src/store/analytics";
import { actions as localStorageActions } from "src/store/localStorage";
import {
  StatementsPage,
  StatementsPageDispatchProps,
  StatementsPageProps,
  StatementsPageStateProps,
} from "./statementsPage";
import {
  selectApps,
  selectLastReset,
  selectStatements,
  selectStatementsLastError,
  selectTotalFingerprints,
} from "./statementsPage.selectors";
import { AggregateStatistics } from "../statementsTable";

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
      totalFingerprints: selectTotalFingerprints(state),
      lastReset: selectLastReset(state),
    }),
    (dispatch: Dispatch) => ({
      refreshStatements: () => dispatch(statementActions.refresh()),
      refreshStatementDiagnosticsRequests: () =>
        dispatch(statementDiagnosticsActions.refresh()),
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
    }),
  )(StatementsPage),
);
