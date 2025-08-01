// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { createSelector } from "@reduxjs/toolkit";
import Long from "long";
import moment from "moment-timezone";
import { RouteComponentProps } from "react-router-dom";

import { AppState } from "../store";
import { selectTimeScale } from "../store/utils/selectors";
import { TimeScale, toRoundedDateRange } from "../timeScaleDropdown";
import {
  appNamesAttr,
  statementAttr,
  getMatchParamByName,
  queryByName,
  generateStmtDetailsToID,
} from "../util";

type StatementDetailsResponseMessage =
  cockroach.server.serverpb.StatementDetailsResponse;

export const selectStatementDetails = createSelector(
  (_state: AppState, props: RouteComponentProps): string =>
    getMatchParamByName(props.match, statementAttr),
  (_state: AppState, props: RouteComponentProps): string =>
    queryByName(props.location, appNamesAttr),
  (state: AppState): TimeScale => selectTimeScale(state),
  (state: AppState) => state.adminUI?.sqlDetailsStats.cachedData,
  (
    fingerprintID,
    appNames,
    timeScale,
    statementDetailsStatsData,
  ): {
    statementDetails: StatementDetailsResponseMessage;
    isLoading: boolean;
    lastError: Error;
    lastUpdated: moment.Moment | null;
  } => {
    // Since the aggregation interval is 1h, we want to round the selected timeScale to include
    // the full hour. If a timeScale is between 14:32 - 15:17 we want to search for values
    // between 14:00 - 16:00. We don't encourage the aggregation interval to be modified, but
    // in case that changes in the future we might consider changing this function to use the
    // cluster settings value for the rounding function.
    const [start, end] = toRoundedDateRange(timeScale);
    const key = generateStmtDetailsToID(
      fingerprintID,
      appNames,
      Long.fromNumber(start.unix()),
      Long.fromNumber(end.unix()),
    );
    if (Object.keys(statementDetailsStatsData).includes(key)) {
      return {
        statementDetails: statementDetailsStatsData[key].data,
        isLoading: statementDetailsStatsData[key].inFlight,
        lastError: statementDetailsStatsData[key].lastError,
        lastUpdated: statementDetailsStatsData[key].lastUpdated,
      };
    }
    return {
      statementDetails: null,
      isLoading: true,
      lastError: null,
      lastUpdated: null,
    };
  },
);

export const selectStatementDetailsUiConfig = createSelector(
  (state: AppState) => state.adminUI?.uiConfig?.pages.statementDetails,
  statementDetailsUiConfig => statementDetailsUiConfig,
);
