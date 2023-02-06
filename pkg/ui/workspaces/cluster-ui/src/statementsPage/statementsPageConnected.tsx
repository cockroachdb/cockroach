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
import { actions as localStorageActions } from "src/store/localStorage";
import { actions as sqlStatsActions } from "src/store/sqlStats";
import { actions as nodesActions } from "../store/nodes";
import {
  StatementsPageDispatchProps,
  StatementsPageStateProps,
} from "./statementsPage";
import {
  selectApps,
  selectDatabases,
  selectLastReset,
  selectStatements,
  selectStatementsDataValid,
  selectStatementsLastError,
  selectTotalFingerprints,
  selectColumns,
  selectTimeScale,
  selectSortSetting,
  selectFilters,
  selectSearch,
  selectStatementsLastUpdated,
} from "./statementsPage.selectors";
import {
  selectIsTenant,
  selectHasViewActivityRedactedRole,
  selectHasAdminRole,
} from "../store/uiConfig";
import { nodeRegionsByIDSelector } from "../store/nodes";
import { StatementsRequest } from "src/api/statementsApi";
import { TimeScale } from "../timeScaleDropdown";
import { cockroach, google } from "@cockroachlabs/crdb-protobuf-client";
import {
  StatementsPageRoot,
  StatementsPageRootProps,
} from "./statementsPageRoot";
import {
  ActiveStatementsViewDispatchProps,
  ActiveStatementsViewStateProps,
} from "./activeStatementsView";
import {
  mapDispatchToActiveStatementsPageProps,
  mapStateToActiveStatementsPageProps,
} from "./activeStatementsPage.selectors";

type IStatementDiagnosticsReport =
  cockroach.server.serverpb.IStatementDiagnosticsReport;
type IDuration = google.protobuf.IDuration;

const CreateStatementDiagnosticsReportRequest =
  cockroach.server.serverpb.CreateStatementDiagnosticsReportRequest;

const CancelStatementDiagnosticsReportRequest =
  cockroach.server.serverpb.CancelStatementDiagnosticsReportRequest;

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
    StatementsPageRootProps
  >(
    (state: AppState, props: RouteComponentProps) => ({
      fingerprintsPageProps: {
        ...props,
        apps: selectApps(state),
        columns: selectColumns(state),
        databases: selectDatabases(state),
        timeScale: selectTimeScale(state),
        filters: selectFilters(state),
        isTenant: selectIsTenant(state),
        hasViewActivityRedactedRole: selectHasViewActivityRedactedRole(state),
        hasAdminRole: selectHasAdminRole(state),
        lastReset: selectLastReset(state),
        nodeRegions: selectIsTenant(state)
          ? {}
          : nodeRegionsByIDSelector(state),
        search: selectSearch(state),
        sortSetting: selectSortSetting(state),
        statements: selectStatements(state, props),
        isDataValid: selectStatementsDataValid(state),
        lastUpdated: selectStatementsLastUpdated(state),
        statementsError: selectStatementsLastError(state),
        totalFingerprints: selectTotalFingerprints(state),
      },
      activePageProps: mapStateToActiveStatementsPageProps(state),
    }),
    (dispatch: Dispatch) => ({
      fingerprintsPageProps: {
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
        resetSQLStats: (req: StatementsRequest) =>
          dispatch(sqlStatsActions.reset(req)),
        dismissAlertMessage: () =>
          dispatch(
            localStorageActions.update({
              key: "adminUi/showDiagnosticsModal",
              value: false,
            }),
          ),
        onActivateStatementDiagnostics: (
          statementFingerprint: string,
          minExecLatency: IDuration,
          expiresAfter: IDuration,
        ) => {
          dispatch(
            statementDiagnosticsActions.createReport(
              new CreateStatementDiagnosticsReportRequest({
                statement_fingerprint: statementFingerprint,
                min_execution_latency: minExecLatency,
                expires_after: expiresAfter,
              }),
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
          report: IStatementDiagnosticsReport,
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
              statementDiagnosticsActions.cancelReport(
                new CancelStatementDiagnosticsReportRequest({
                  request_id: report.id,
                }),
              ),
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
      },
      activePageProps: mapDispatchToActiveStatementsPageProps(dispatch),
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
