// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { createSelector } from "reselect";
import { formatDate, TimestampToMoment } from "src/util";
import { RouteComponentProps } from "react-router-dom";

import { AppState } from "src/store";
import { selectDiagnosticsReportsPerStatement } from "../store/statementDiagnostics";
import { sqlStatsSelector } from "../store/sqlStats/sqlStats.selector";
import { SQLStatsState } from "../store/sqlStats";
import { localStorageSelector } from "../store/utils/selectors";
import { databasesListSelector } from "src/store/databasesList/databasesList.selectors";
import { selectStmtsCombiner } from "src/selectors/common";

export const selectStatementsLastUpdated = createSelector(
  sqlStatsSelector,
  sqlStats => sqlStats.lastUpdated,
);

// selectDatabases returns the array of all databases in the cluster.
export const selectDatabases = createSelector(databasesListSelector, state => {
  if (!state?.data) {
    return [];
  }

  return state.data.databases
    .filter((dbName: string) => dbName !== null && dbName.length > 0)
    .sort();
});

// selectLastReset returns a string displaying the last time the statement
// statistics were reset.
export const selectLastReset = createSelector(sqlStatsSelector, state => {
  if (!state?.data) {
    return "";
  }

  return formatDate(TimestampToMoment(state.data.last_reset));
});

export const selectStatementsDataValid = createSelector(
  sqlStatsSelector,
  (state: SQLStatsState): boolean => {
    return state.valid;
  },
);

export const selectStatementsDataInFlight = createSelector(
  sqlStatsSelector,
  (state: SQLStatsState): boolean => {
    return state.inFlight;
  },
);

export const selectStatements = createSelector(
  (state: AppState) => state.adminUI?.statements?.data,
  (_: AppState, props: RouteComponentProps) => props,
  selectDiagnosticsReportsPerStatement,
  selectStmtsCombiner,
);

export const selectStatementsLastError = createSelector(
  sqlStatsSelector,
  state => state.lastError,
);

export const selectColumns = createSelector(
  localStorageSelector,
  // return array of columns if user have customized it or `null` otherwise
  localStorage =>
    localStorage["showColumns/StatementsPage"]
      ? localStorage["showColumns/StatementsPage"]?.split(",")
      : null,
);

export const selectSortSetting = createSelector(
  localStorageSelector,
  localStorage => localStorage["sortSetting/StatementsPage"],
);

export const selectFilters = createSelector(
  localStorageSelector,
  localStorage => localStorage["filters/StatementsPage"],
);

export const selectSearch = createSelector(
  localStorageSelector,
  localStorage => localStorage["search/StatementsPage"],
);
