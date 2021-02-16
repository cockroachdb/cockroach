// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { RouteComponentProps, withRouter } from "react-router-dom";
import { connect } from "react-redux";
import { AppState } from "src/store";
import { SessionsState } from "src/store/sessions";

import { createSelector } from "reselect";
import { SessionsPage } from "./index";

import { actions as sessionsActions } from "src/store/sessions";
import { actions as TerminateQueryActions } from "src/store/terminateQuery";

export const selectSessions = createSelector(
  (state: AppState) => state.adminUI.sessions,
  (state: SessionsState) => {
    if (!state.data) {
      return null;
    }
    return state.data.sessions.map(session => {
      return { session };
    });
  },
);

export const SessionsPageConnected = withRouter(
  connect(
    (state: AppState, props: RouteComponentProps) => ({
      sessions: selectSessions(state),
      sessionsError: state.adminUI.sessions.lastError,
    }),
    {
      refreshSessions: sessionsActions.refresh,
      cancelSession: TerminateQueryActions.terminateSession,
      cancelQuery: TerminateQueryActions.terminateQuery,
    },
  )(SessionsPage),
);
