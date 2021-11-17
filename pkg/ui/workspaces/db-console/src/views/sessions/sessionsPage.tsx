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
import { SessionsResponseMessage } from "src/util/api";

import { SessionsPage } from "@cockroachlabs/cluster-ui";
import {
  terminateQueryAction,
  terminateSessionAction,
} from "src/redux/sessions/sessionsSagas";

type SessionsState = Pick<AdminUIState, "cachedData", "sessions">;

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

export const sortSettingLocalSetting = new LocalSetting(
  "sortSetting/SessionsPage",
  (state: AdminUIState) => state.localSettings,
  { ascending: false, columnTitle: "statementAge" },
);

const SessionsPageConnected = withRouter(
  connect(
    (state: AdminUIState, props: RouteComponentProps) => ({
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
    },
  )(SessionsPage),
);

export default SessionsPageConnected;
