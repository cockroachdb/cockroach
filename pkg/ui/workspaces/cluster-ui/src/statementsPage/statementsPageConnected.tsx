// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { Dispatch } from "redux";

import { AppState } from "src/store";
import { actions as analyticsActions } from "src/store/analytics";
import {
  actions as localStorageActions,
  updateStmtsPageLimitAction,
  updateStmsPageReqSortAction,
} from "src/store/localStorage";
import { actions as sqlStatsActions } from "src/store/sqlStats";

import { StatementDiagnosticsReport, SqlStatsSortType } from "../api";
import { Filters } from "../queryFilter";
import {
  selectTimeScale,
  selectStmtsPageLimit,
  selectStmtsPageReqSort,
} from "../store/utils/selectors";
import { TimeScale } from "../timeScaleDropdown";

import {
  StatementsPageDispatchProps,
  StatementsPageStateProps,
} from "./statementsPage";
import {
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
};

type DispatchProps = {
  fingerprintsPageProps: StatementsPageDispatchProps;
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
        timeScale: selectTimeScale(state),
        filters: selectFilters(state),
        search: selectSearch(state),
        sortSetting: selectSortSetting(state),
        limit: selectStmtsPageLimit(state),
        requestTime: selectRequestTime(state),
        reqSortSetting: selectStmtsPageReqSort(state),
      },
    }),
    (dispatch: Dispatch) => ({
      fingerprintsPageProps: {
        onTimeScaleChange: (ts: TimeScale) => {
          dispatch(
            sqlStatsActions.updateTimeScale({
              ts: ts,
            }),
          );
        },
        onActivateStatementDiagnosticsAnalytics: () =>
          dispatch(
            analyticsActions.track({
              name: "Statement Diagnostics Clicked",
              page: "Statements",
              action: "Activated",
            }),
          ),
        onDiagnosticsReportDropdownOptionAnalytics: (
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
        onFilterChange: (value: Filters) => {
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
    }),
    (stateProps, dispatchProps): StatementsPageRootProps => ({
      fingerprintsPageProps: {
        ...stateProps.fingerprintsPageProps,
        ...dispatchProps.fingerprintsPageProps,
      },
    }),
  )(StatementsPageRoot),
);
