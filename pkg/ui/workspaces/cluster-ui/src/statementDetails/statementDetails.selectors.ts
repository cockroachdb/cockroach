// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { createSelector } from "@reduxjs/toolkit";
import { RouteComponentProps } from "react-router-dom";
import { AppState } from "../store";
import { appNamesAttr, fingerprintIDAttr, getMatchParamByName } from "../util";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
type StatementDetailsResponseMessage = cockroach.server.serverpb.StatementDetailsResponse;

export const generateStmtDetailsToID = (
  fingerprintID: string,
  appNames: string,
): string => {
  if (
    appNames &&
    (appNames.includes("$ internal") || appNames.includes("unset"))
  ) {
    const apps = appNames.split(",");
    for (let i = 0; i < apps.length; i++) {
      if (apps[i].includes("$ internal")) {
        apps[i] = "$ internal";
      }
      if (apps[i].includes("unset")) {
        apps[i] = "";
      }
    }
    appNames = apps.toString();
  }
  if (appNames) {
    return fingerprintID + appNames;
  }
  return fingerprintID;
};

export const selectStatementDetails = createSelector(
  (_state: AppState, props: RouteComponentProps): string =>
    getMatchParamByName(props.match, fingerprintIDAttr),
  (_state: AppState, props: RouteComponentProps): string =>
    getMatchParamByName(props.match, appNamesAttr),
  (state: AppState) => state.adminUI.sqlDetailsStats,
  (
    fingerprintID,
    appNames,
    statementDetailsStats,
  ): StatementDetailsResponseMessage => {
    const key = generateStmtDetailsToID(fingerprintID, appNames);
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
