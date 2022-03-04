// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import Long from "long";
import { createSelector } from "@reduxjs/toolkit";
import { RouteComponentProps } from "react-router-dom";
import { AppState } from "../store";
import {
  appNamesAttr,
  fingerprintIDAttr,
  generateStmtDetailsToID,
  getMatchParamByName,
} from "../util";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { TimeScale, toRoundedDateRange } from "../timeScaleDropdown";
import { selectTimeScale } from "../statementsPage/statementsPage.selectors";
type StatementDetailsResponseMessage = cockroach.server.serverpb.StatementDetailsResponse;

export const selectStatementDetails = createSelector(
  (_state: AppState, props: RouteComponentProps): string =>
    getMatchParamByName(props.match, fingerprintIDAttr),
  (_state: AppState, props: RouteComponentProps): string =>
    getMatchParamByName(props.match, appNamesAttr),
  (state: AppState): TimeScale => selectTimeScale(state),
  (state: AppState) => state.adminUI.sqlDetailsStats,
  (
    fingerprintID,
    appNames,
    timeScale,
    statementDetailsStats,
  ): StatementDetailsResponseMessage => {
    const [start, end] = toRoundedDateRange(timeScale);
    const key = generateStmtDetailsToID(
      fingerprintID,
      appNames,
      Long.fromNumber(start.unix()),
      Long.fromNumber(end.unix()),
    );
    if (Object.keys(statementDetailsStats).includes(key)) {
      return statementDetailsStats[key].data;
    }
    return null;
  },
);

export const selectStatementDetailsUiConfig = createSelector(
  (state: AppState) => state.adminUI.uiConfig.pages.statementDetails,
  statementDetailsUiConfig => statementDetailsUiConfig,
);
