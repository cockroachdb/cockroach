// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { SessionDetails, byteArrayToUuid } from "@cockroachlabs/cluster-ui";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { createSelector } from "reselect";

import {
  CachedDataReducerState,
  refreshLiveness,
  refreshNodes,
  refreshSessions,
} from "src/redux/apiReducers";
import { nodeDisplayNameByIDSelectorWithoutAddress } from "src/redux/nodes";
import {
  terminateQueryAction,
  terminateSessionAction,
} from "src/redux/sessions/sessionsSagas";
import { AdminUIState } from "src/redux/state";
import { setTimeScale } from "src/redux/timeScale";
import { SessionsResponseMessage } from "src/util/api";
import { sessionAttr } from "src/util/constants";
import { Pick } from "src/util/pick";
import { getMatchParamByName } from "src/util/query";

type SessionsState = Pick<AdminUIState, "cachedData", "sessions">;

export const selectSession = createSelector(
  (state: SessionsState) => state.cachedData.sessions,
  (_state: SessionsState, props: RouteComponentProps) => props,
  (
    state: CachedDataReducerState<SessionsResponseMessage>,
    props: RouteComponentProps<any>,
  ) => {
    if (!state?.data) {
      return null;
    }
    const sessionID = getMatchParamByName(props.match, sessionAttr);
    return {
      session: state.data.sessions.find(
        session => byteArrayToUuid(session.id) === sessionID,
      ),
    };
  },
);
const SessionDetailsPageConnected = withRouter(
  connect(
    (state: AdminUIState, props: RouteComponentProps) => ({
      nodeNames: nodeDisplayNameByIDSelectorWithoutAddress(state),
      session: selectSession(state, props),
      sessionError: state.cachedData.sessions.lastError,
    }),
    {
      refreshSessions,
      cancelSession: terminateSessionAction,
      cancelQuery: terminateQueryAction,
      refreshNodes: refreshNodes,
      refreshNodesLiveness: refreshLiveness,
      setTimeScale,
    },
  )(SessionDetails),
);

export default SessionDetailsPageConnected;
