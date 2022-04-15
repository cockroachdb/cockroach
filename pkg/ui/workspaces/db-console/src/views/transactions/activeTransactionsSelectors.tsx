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
  ActiveTransactionFilters,
  ActiveTransaction,
  getActiveTransactionsFromSessions,
  SortSetting,
  defaultFilters,
} from "@cockroachlabs/cluster-ui";
import { createSelector } from "reselect";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";
import { SessionsResponseMessage } from "src/util/api";
import { refreshSessions } from "src/redux/apiReducers";

const selectActiveTransactions = createSelector(
  (state: AdminUIState) => state.cachedData.sessions,
  (
    state?: CachedDataReducerState<SessionsResponseMessage>,
  ): ActiveTransaction[] => {
    if (state?.data == null) return [];
    return getActiveTransactionsFromSessions(state.data, state.setAt);
  },
);

const transactionsColumnsLocalSetting = new LocalSetting<
  AdminUIState,
  string | null
>(
  "columns/ActiveTransactionsPage",
  (state: AdminUIState) => state.localSettings,
  null,
);

const filtersLocalSetting = new LocalSetting<
  AdminUIState,
  ActiveTransactionFilters
>(
  "filters/ActiveTransactionsPage",
  (state: AdminUIState) => state.localSettings,
  { app: defaultFilters.app },
);

const sortSettingLocalSetting = new LocalSetting<AdminUIState, SortSetting>(
  "sortSetting/ActiveTransactionsPage",
  (state: AdminUIState) => state.localSettings,
  { ascending: false, columnTitle: "startTime" },
);

export const mapStateToActiveTransactionsPageProps = (state: AdminUIState) => ({
  selectedColumns: transactionsColumnsLocalSetting.selectorToArray(state),
  transactions: selectActiveTransactions(state),
  sessionsError: state.cachedData?.sessions.lastError,
  filters: filtersLocalSetting.selector(state),
  sortSetting: sortSettingLocalSetting.selector(state),
});

export const activeTransactionsPageActions = {
  onColumnsSelect: (columns: string[]) =>
    transactionsColumnsLocalSetting.set(columns.join(",")),
  refreshSessions,
  onFiltersChange: (filters: ActiveTransactionFilters) =>
    filtersLocalSetting.set(filters),
  onSortChange: (ss: SortSetting) => sortSettingLocalSetting.set(ss),
};
