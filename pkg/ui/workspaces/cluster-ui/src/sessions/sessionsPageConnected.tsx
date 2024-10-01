// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { Dispatch } from "redux";
import { createSelector } from "reselect";

import { analyticsActions, AppState } from "src/store";
import { actions as localStorageActions } from "src/store/localStorage";
import { SessionsState, actions as sessionsActions } from "src/store/sessions";
import {
  actions as terminateQueryActions,
  ICancelQueryRequest,
  ICancelSessionRequest,
} from "src/store/terminateQuery";

import { Filters } from "../queryFilter";
import { sqlStatsSelector } from "../store/sqlStats/sqlStats.selector";
import { localStorageSelector } from "../store/utils/selectors";

import { SessionsPage } from "./index";

export const selectSessionsData = createSelector(
  sqlStatsSelector,
  sessionsState => (sessionsState.valid ? sessionsState.data : null),
);

export const selectSessions = createSelector(
  (state: AppState) => state.adminUI?.sessions,
  (state: SessionsState) => {
    if (!state?.data) {
      return null;
    }
    return state.data.sessions.map(session => {
      return { session };
    });
  },
);

export const selectAppName = createSelector(
  (state: AppState) => state.adminUI?.sessions,
  (state: SessionsState) => {
    if (!state?.data) {
      return null;
    }
    return state.data.internal_app_name_prefix;
  },
);

export const selectSortSetting = createSelector(
  (state: AppState) => state.adminUI?.localStorage,
  localStorage => localStorage["sortSetting/SessionsPage"],
);

export const selectColumns = createSelector(
  localStorageSelector,
  localStorage =>
    localStorage["showColumns/SessionsPage"]
      ? localStorage["showColumns/SessionsPage"]?.split(",")
      : null,
);

export const selectFilters = createSelector(
  localStorageSelector,
  localStorage => localStorage["filters/SessionsPage"],
);

export const SessionsPageConnected = withRouter(
  connect(
    (state: AppState, _props: RouteComponentProps) => ({
      sessions: selectSessions(state),
      internalAppNamePrefix: selectAppName(state),
      sessionsError: state.adminUI?.sessions.lastError,
      sortSetting: selectSortSetting(state),
      columns: selectColumns(state),
      filters: selectFilters(state),
    }),
    (dispatch: Dispatch) => ({
      refreshSessions: () => dispatch(sessionsActions.refresh()),
      cancelSession: (payload: ICancelSessionRequest) =>
        dispatch(terminateQueryActions.terminateSession(payload)),
      cancelQuery: (payload: ICancelQueryRequest) =>
        dispatch(terminateQueryActions.terminateQuery(payload)),
      onSortingChange: (
        tableName: string,
        columnName: string,
        ascending: boolean,
      ) => {
        dispatch(
          analyticsActions.track({
            name: "Column Sorted",
            page: "Sessions",
            tableName,
            columnName,
          }),
        );
        dispatch(
          localStorageActions.update({
            key: "sortSetting/SessionsPage",
            value: { columnTitle: columnName, ascending: ascending },
          }),
        );
      },
      onSessionClick: () => {
        dispatch(
          analyticsActions.track({
            name: "Session Clicked",
            page: "Sessions",
          }),
        );
      },
      onTerminateSessionClick: () =>
        analyticsActions.track({
          name: "Session Actions Clicked",
          page: "Sessions",
          action: "Cancel Session",
        }),
      onTerminateStatementClick: () =>
        analyticsActions.track({
          name: "Session Actions Clicked",
          page: "Sessions",
          action: "Cancel Statement",
        }),
      onFilterChange: (value: Filters) => {
        dispatch(
          analyticsActions.track({
            name: "Filter Clicked",
            page: "Sessions",
            filterName: "filters",
            value: value.toString(),
          }),
        );
        dispatch(
          localStorageActions.update({
            key: "filters/SessionsPage",
            value: value,
          }),
        );
      },
      onColumnsChange: (selectedColumns: string[]) =>
        dispatch(
          localStorageActions.update({
            key: "showColumns/SessionsPage",
            value:
              selectedColumns.length === 0 ? " " : selectedColumns.join(","),
          }),
        ),
    }),
  )(SessionsPage),
);
