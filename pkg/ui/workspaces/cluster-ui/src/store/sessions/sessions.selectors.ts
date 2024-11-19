// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { RouteComponentProps } from "react-router-dom";
import { createSelector } from "reselect";

import { byteArrayToUuid } from "src/sessions/sessionsTable";
import { AppState } from "src/store";
import { SessionsState } from "src/store/sessions";
import { sessionAttr } from "src/util/constants";
import { getMatchParamByName } from "src/util/query";

export const selectSession = createSelector(
  (state: AppState) => state.adminUI?.sessions,
  (_state: AppState, props: RouteComponentProps) => props,
  (state: SessionsState, props: RouteComponentProps<any>) => {
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

export const selectSessionDetailsUiConfig = createSelector(
  (state: AppState) => state.adminUI?.uiConfig?.pages.sessionDetails,
  statementDetailsUiConfig => statementDetailsUiConfig,
);
