// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { analyticsActions, AppState } from "src/store";
import { SessionDetails } from ".";
import {
  actions as sessionsActions,
  selectSession,
  selectSessionDetailsUiConfig,
} from "src/store/sessions";
import { actions as terminateQueryActions } from "src/store/terminateQuery";
import { actions as nodesActions } from "src/store/nodes";
import { actions as localStorageActions } from "src/store/localStorage";
import { actions as nodesLivenessActions } from "src/store/liveness";

import { nodeDisplayNameByIDSelector } from "src/store/nodes";
import { selectIsTenant } from "../store/uiConfig";
import { TimeScale } from "src/timeScaleDropdown";

export const SessionDetailsPageConnected = withRouter(
  connect(
    (state: AppState, props: RouteComponentProps) => ({
      nodeNames: selectIsTenant(state)
        ? {}
        : nodeDisplayNameByIDSelector(state),
      session: selectSession(state, props),
      sessionError: state.adminUI?.sessions.lastError,
      uiConfig: selectSessionDetailsUiConfig(state),
      isTenant: selectIsTenant(state),
    }),
    {
      refreshSessions: sessionsActions.refresh,
      cancelSession: terminateQueryActions.terminateSession,
      cancelQuery: terminateQueryActions.terminateQuery,
      refreshNodes: nodesActions.refresh,
      refreshNodesLiveness: nodesLivenessActions.refresh,
      setTimeScale: (ts: TimeScale) =>
        localStorageActions.updateTimeScale({
          value: ts,
        }),
      onTerminateSessionClick: () =>
        analyticsActions.track({
          name: "Session Actions Clicked",
          page: "Sessions Details",
          action: "Cancel Session",
        }),
      onTerminateStatementClick: () =>
        analyticsActions.track({
          name: "Session Actions Clicked",
          page: "Sessions Details",
          action: "Cancel Statement",
        }),
      onBackButtonClick: () =>
        analyticsActions.track({
          name: "Back Clicked",
          page: "Sessions Details",
        }),
      onStatementClick: () =>
        analyticsActions.track({
          name: "Statement Clicked",
          page: "Sessions Details",
        }),
    },
  )(SessionDetails),
);
