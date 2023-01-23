// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { connect } from "react-redux";
import { createSelector } from "reselect";
import { withRouter } from "react-router-dom";
import {
  refreshNodes,
  refreshStatements,
  refreshUserSQLRoles,
} from "src/redux/apiReducers";
import { resetSQLStatsAction } from "src/redux/sqlStats";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { AdminUIState } from "src/redux/state";
import { selectHasAdminRole } from "src/redux/user";
import { StatementsResponseMessage } from "src/util/api";

import { PrintTime } from "src/views/reports/containers/range/print";

import {
  TransactionsPage,
  Filters,
  defaultFilters,
  util,
} from "@cockroachlabs/cluster-ui";
import { nodeRegionsByIDSelector } from "src/redux/nodes";
import { setGlobalTimeScaleAction } from "src/redux/statements";
import { LocalSetting } from "src/redux/localsettings";
import { selectTimeScale } from "src/redux/timeScale";

// selectStatements returns the array of AggregateStatistics to show on the
// TransactionsPage, based on if the appAttr route parameter is set.
export const selectData = createSelector(
  (state: AdminUIState) => state.cachedData.statements,
  (state: CachedDataReducerState<StatementsResponseMessage>) => {
    if (!state.data || state.inFlight || !state.valid) return null;
    return state.data;
  },
);

// selectLastReset returns a string displaying the last time the statement
// statistics were reset.
export const selectLastReset = createSelector(
  (state: AdminUIState) => state.cachedData.statements,
  (state: CachedDataReducerState<StatementsResponseMessage>) => {
    if (!state.data) {
      return "unknown";
    }

    return PrintTime(util.TimestampToMoment(state.data.last_reset));
  },
);

export const selectLastError = createSelector(
  (state: AdminUIState) => state.cachedData.statements,
  (state: CachedDataReducerState<StatementsResponseMessage>) => state.lastError,
);

export const sortSettingLocalSetting = new LocalSetting(
  "sortSetting/TransactionsPage",
  (state: AdminUIState) => state.localSettings,
  { ascending: false, columnTitle: "executionCount" },
);

export const filtersLocalSetting = new LocalSetting(
  "filters/TransactionsPage",
  (state: AdminUIState) => state.localSettings,
  defaultFilters,
);

export const searchLocalSetting = new LocalSetting(
  "search/TransactionsPage",
  (state: AdminUIState) => state.localSettings,
  null,
);

export const transactionColumnsLocalSetting = new LocalSetting(
  "showColumns/TransactionPage",
  (state: AdminUIState) => state.localSettings,
  null,
);

const TransactionsPageConnected = withRouter(
  connect(
    (state: AdminUIState) => ({
      columns: transactionColumnsLocalSetting.selectorToArray(state),
      data: selectData(state),
      timeScale: selectTimeScale(state),
      error: selectLastError(state),
      filters: filtersLocalSetting.selector(state),
      lastReset: selectLastReset(state),
      nodeRegions: nodeRegionsByIDSelector(state),
      search: searchLocalSetting.selector(state),
      sortSetting: sortSettingLocalSetting.selector(state),
      statementsError: state.cachedData.statements.lastError,
      hasAdminRole: selectHasAdminRole(state),
    }),
    {
      refreshData: refreshStatements,
      refreshNodes,
      refreshUserSQLRoles,
      resetSQLStats: resetSQLStatsAction,
      onTimeScaleChange: setGlobalTimeScaleAction,
      // We use `null` when the value was never set and it will show all columns.
      // If the user modifies the selection and no columns are selected,
      // the function will save the value as a blank space, otherwise
      // it gets saved as `null`.
      onColumnsChange: (value: string[]) =>
        transactionColumnsLocalSetting.set(
          value.length === 0 ? " " : value.join(","),
        ),
      onSortingChange: (
        _tableName: string,
        columnName: string,
        ascending: boolean,
      ) =>
        sortSettingLocalSetting.set({
          ascending: ascending,
          columnTitle: columnName,
        }),
      onFilterChange: (filters: Filters) => filtersLocalSetting.set(filters),
      onSearchComplete: (query: string) => searchLocalSetting.set(query),
    },
  )(TransactionsPage),
);

export default TransactionsPageConnected;
