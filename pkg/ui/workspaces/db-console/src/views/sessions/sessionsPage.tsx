// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  defaultFiltersForSessionsPage,
  Filters,
  SessionsPage,
} from "@cockroachlabs/cluster-ui";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { Action } from "redux";
import { ThunkDispatch } from "redux-thunk";
import { createSelector } from "reselect";

import * as protos from "src/js/protos";
import { CachedDataReducerState, refreshSessions } from "src/redux/apiReducers";
import { LocalSetting } from "src/redux/localsettings";
import {
  terminateQueryAction,
  terminateSessionAction,
} from "src/redux/sessions/sessionsSagas";
import { AdminUIState } from "src/redux/state";
import { SessionsResponseMessage } from "src/util/api";
import { Pick } from "src/util/pick";

const SessionsRequest = protos.cockroach.server.serverpb.ListSessionsRequest;

type SessionsState = Pick<AdminUIState, "cachedData", "sessions">;

export const selectData = createSelector(
  (state: AdminUIState) => state.cachedData.statements,
  state => {
    if (!state.data || state.inFlight || !state.valid) return null;
    return state.data;
  },
);

export const selectSessions = createSelector(
  (state: SessionsState) => state.cachedData.sessions,
  (_state: SessionsState, props: RouteComponentProps) => props,
  (
    state: CachedDataReducerState<SessionsResponseMessage>,
    _: RouteComponentProps<any>,
  ) => {
    if (!state?.data) {
      return null;
    }
    return state.data.sessions.map(session => {
      return { session };
    });
  },
);

export const selectAppName = createSelector(
  (state: SessionsState) => state.cachedData.sessions,
  (_state: SessionsState, props: RouteComponentProps) => props,
  (
    state: CachedDataReducerState<SessionsResponseMessage>,
    _: RouteComponentProps<any>,
  ) => {
    if (!state?.data) {
      return null;
    }
    return state.data.internal_app_name_prefix;
  },
);

export const sortSettingLocalSetting = new LocalSetting(
  "sortSetting/SessionsPage",
  (state: AdminUIState) => state.localSettings,
  { ascending: false, columnTitle: "statementAge" },
);

export const sessionColumnsLocalSetting = new LocalSetting(
  "showColumns/SessionsPage",
  (state: AdminUIState) => state.localSettings,
  null,
);

export const filtersLocalSetting = new LocalSetting<AdminUIState, Filters>(
  "filters/SessionsPage",
  (state: AdminUIState) => state.localSettings,
  defaultFiltersForSessionsPage,
);

// Interface matching cluster-ui's SessionsRequest
interface ClusterUiSessionsRequest {
  excludeClosedSessions?: boolean;
}

const SessionsPageConnected = withRouter(
  connect(
    (state: AdminUIState, props: RouteComponentProps) => ({
      columns: sessionColumnsLocalSetting.selectorToArray(state),
      internalAppNamePrefix: selectAppName(state, props),
      filters: filtersLocalSetting.selector(state),
      sessions: selectSessions(state, props),
      sessionsError: state.cachedData.sessions.lastError,
      sortSetting: sortSettingLocalSetting.selector(state),
    }),
    (dispatch: ThunkDispatch<AdminUIState, unknown, Action>) => ({
      refreshSessions: (req?: ClusterUiSessionsRequest) => {
        const protoReq = new SessionsRequest({
          exclude_closed_sessions: req?.excludeClosedSessions,
        });
        dispatch(refreshSessions(protoReq));
      },
      cancelSession: terminateSessionAction,
      cancelQuery: terminateQueryAction,
      onSortingChange: (
        _tableName: string,
        columnName: string,
        ascending: boolean,
      ) =>
        dispatch(
          sortSettingLocalSetting.set({
            ascending: ascending,
            columnTitle: columnName,
          }),
        ),
      onColumnsChange: (value: string[]) =>
        dispatch(
          sessionColumnsLocalSetting.set(
            value.length === 0 ? " " : value.join(","),
          ),
        ),
      onFilterChange: (filters: Filters) =>
        dispatch(filtersLocalSetting.set(filters)),
    }),
  )(SessionsPage),
);

export default SessionsPageConnected;
