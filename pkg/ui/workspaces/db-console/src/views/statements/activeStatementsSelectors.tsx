// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import {
  ActiveStatementFilters,
  ActiveStatement,
  getActiveStatementsFromSessions,
  defaultFilters,
  SortSetting,
} from "@cockroachlabs/cluster-ui";
import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";
import { CachedDataReducerState, refreshSessions } from "src/redux/apiReducers";
import { createSelector } from "reselect";
import { SessionsResponseMessage } from "src/util/api";

const selectActiveStatements = createSelector(
  (state: AdminUIState) => state.cachedData.sessions,
  (
    sessions?: CachedDataReducerState<SessionsResponseMessage>,
  ): ActiveStatement[] => {
    if (sessions?.data == null) return [];

    return getActiveStatementsFromSessions(sessions.data, sessions.setAt);
  },
);

export const selectAppName = createSelector(
  (state: AdminUIState) => state.cachedData.sessions,
  (state?: CachedDataReducerState<SessionsResponseMessage>) => {
    if (!state.data) {
      return null;
    }
    return state.data.internal_app_name_prefix;
  },
);

const selectedColumnsLocalSetting = new LocalSetting<
  AdminUIState,
  string | null
>(
  "columns/ActiveStatementsPage",
  (state: AdminUIState) => state.localSettings,
  null,
);

const filtersLocalSetting = new LocalSetting<
  AdminUIState,
  ActiveStatementFilters
>(
  "filters/ActiveStatementsPage",
  (state: AdminUIState) => state.localSettings,
  { app: defaultFilters.app },
);

const sortSettingLocalSetting = new LocalSetting<AdminUIState, SortSetting>(
  "sortSetting/ActiveStatementsPage",
  (state: AdminUIState) => state.localSettings,
  { ascending: false, columnTitle: "startTime" },
);

export const mapStateToActiveStatementViewProps = (state: AdminUIState) => ({
  filters: filtersLocalSetting.selector(state),
  selectedColumns: selectedColumnsLocalSetting.selectorToArray(state),
  sortSetting: sortSettingLocalSetting.selector(state),
  statements: selectActiveStatements(state),
  sessionsError: state.cachedData?.sessions.lastError,
  internalAppNamePrefix: selectAppName(state),
});

export const activeStatementsViewActions = {
  onColumnsSelect: (columns: string[]) =>
    selectedColumnsLocalSetting.set(columns.join(",")),
  refreshSessions,
  onFiltersChange: (filters: ActiveStatementFilters) =>
    filtersLocalSetting.set(filters),
  onSortChange: (ss: SortSetting) => sortSettingLocalSetting.set(ss),
};
