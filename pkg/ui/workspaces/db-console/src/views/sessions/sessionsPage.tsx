// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Pick } from "src/util/pick";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { connect } from "react-redux";
import { AdminUIState } from "src/redux/state";
import { LocalSetting } from "src/redux/localsettings";
import { CachedDataReducerState, refreshSessions } from "src/redux/apiReducers";

import { createSelector } from "reselect";
import {
  SessionsResponseMessage,
  StatementsResponseMessage,
} from "src/util/api";

import {
  defaultFilters,
  Filters,
  SessionsPage,
} from "@cockroachlabs/cluster-ui";
import {
  terminateQueryAction,
  terminateSessionAction,
} from "src/redux/sessions/sessionsSagas";

type SessionsState = Pick<AdminUIState, "cachedData", "sessions">;

export const selectData = createSelector(
  (state: AdminUIState) => state.cachedData.statements,
  (state: CachedDataReducerState<StatementsResponseMessage>) => {
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
    if (!state.data) {
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
    if (!state.data) {
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

export const filtersLocalSetting = new LocalSetting(
  "filters/SessionsPage",
  (state: AdminUIState) => state.localSettings,
  defaultFilters,
);

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
    {
      refreshSessions,
      cancelSession: terminateSessionAction,
      cancelQuery: terminateQueryAction,
      onSortingChange: (
        _tableName: string,
        columnName: string,
        ascending: boolean,
      ) =>
        sortSettingLocalSetting.set({
          ascending: ascending,
          columnTitle: columnName,
        }),
      onColumnsChange: (value: string[]) =>
        sessionColumnsLocalSetting.set(
          value.length === 0 ? " " : value.join(","),
        ),
      onFilterChange: (filters: Filters) => filtersLocalSetting.set(filters),
    },
  )(SessionsPage),
);

export default SessionsPageConnected;
