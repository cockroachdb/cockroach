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
import { getActiveStatementsFromSessions } from "../activeExecutions/activeStatementUtils";
import { localStorageSelector } from "./statementsPage.selectors";

export const selectActiveStatements = createSelector(
  (state: AppState) => state.adminUI.sessions,
  response => {
    if (!response.data) return null;

    return getActiveStatementsFromSessions(response.data, response.lastUpdated);
  },
);

export const selectSortSetting = createSelector(
  localStorageSelector,
  localStorage => localStorage["sortSetting/ActiveStatementsPage"],
);

export const selectFilters = createSelector(
  localStorageSelector,
  localStorage => localStorage["filters/ActiveStatementsPage"],
);

export const selectColumns = createSelector(
  localStorageSelector,
  localStorage => {
    const value = localStorage["showColumns/ActiveStatementsPage"];

    if (value == null) return null;

    return value.split(",").map(col => col.trim());
  },
);
