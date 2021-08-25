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
import { analyticsActions, AppState } from "src/store";
import { SessionsState } from "src/store/sessions";

import { createSelector } from "reselect";
import { SessionsPage } from "./index";

import { actions as sessionsActions } from "src/store/sessions";
import {
  actions as terminateQueryActions,
  ICancelQueryRequest,
  ICancelSessionRequest,
} from "src/store/terminateQuery";
import { Dispatch } from "redux";

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
    (dispatch: Dispatch) => ({
      refreshSessions: () => dispatch(sessionsActions.refresh()),
      cancelSession: (payload: ICancelSessionRequest) =>
        dispatch(terminateQueryActions.terminateSession(payload)),
      cancelQuery: (payload: ICancelQueryRequest) =>
        dispatch(terminateQueryActions.terminateQuery(payload)),
      onSortingChange: (columnName: string) => {
        dispatch(
          analyticsActions.track({
            name: "Column Sorted",
            page: "Sessions",
            columnName,
            tableName: "Sessions",
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
          action: "Terminate Session",
        }),
      onTerminateStatementClick: () =>
        analyticsActions.track({
          name: "Session Actions Clicked",
          page: "Sessions",
          action: "Terminate Statement",
        }),
    }),
  )(SessionsPage),
);
