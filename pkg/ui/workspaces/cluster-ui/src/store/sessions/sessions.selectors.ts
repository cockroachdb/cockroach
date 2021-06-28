// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { AppState } from "src/store";
import { createSelector } from "reselect";
import { RouteComponentProps } from "react-router-dom";
import { SessionsState } from "src/store/sessions";
import { sessionAttr } from "src/util/constants";
import { getMatchParamByName } from "src/util/query";
import { byteArrayToUuid } from "src/sessions/sessionsTable";

export const selectSession = createSelector(
  (state: AppState) => state.adminUI.sessions,
  (_state: AppState, props: RouteComponentProps) => props,
  (state: SessionsState, props: RouteComponentProps<any>) => {
    if (!state.data) {
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
  (state: AppState) => state.adminUI.uiConfig.pages.sessionDetails,
  statementDetailsUiConfig => statementDetailsUiConfig,
);
