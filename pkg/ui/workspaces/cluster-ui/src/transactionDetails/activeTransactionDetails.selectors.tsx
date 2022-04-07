// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { createSelector } from "reselect";
import { AppState } from "src";
import { match, RouteComponentProps } from "react-router-dom";
import { getMatchParamByName } from "src/util/query";
import { executionIdAttr } from "../util/constants";
import { getActiveTransactionsFromSessions } from "../activeExecutions/activeStatementUtils";

export const selectActiveTransaction = createSelector(
  (_: AppState, props: RouteComponentProps) => props.match,
  (state: AppState) => state.adminUI.sessions,
  (match: match, response) => {
    if (!response.data) return null;

    const executionID = getMatchParamByName(match, executionIdAttr);
    return getActiveTransactionsFromSessions(
      response.data,
      response.lastUpdated,
    ).find(stmt => stmt.executionID === executionID);
  },
);
