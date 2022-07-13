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
  SortSetting,
  defaultFilters,
  ActiveTransactionsViewDispatchProps,
  selectActiveTransactionCombiner,
  selectActiveTransactionsCombiner,
  selectContentionDetailsCombiner,
} from "@cockroachlabs/cluster-ui";
import { createSelector } from "reselect";
import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";
import { refreshLiveWorkload } from "src/redux/apiReducers";
import { selectAppName } from "src/views/statements/activeStatementsSelectors";
import { RouteComponentProps } from "react-router-dom";

const selectSessions = (state: AdminUIState) => state.cachedData.sessions?.data;
const selectSessionsLastUpdated = (state: AdminUIState) =>
  state.cachedData.sessions?.setAt;

const selectClusterLocks = (state: AdminUIState) =>
  state.cachedData.clusterLocks?.data;

const selectRouterProps = (_state: AdminUIState, props: RouteComponentProps) =>
  props;

export const selectActiveTransactions = createSelector(
  selectSessions,
  selectSessionsLastUpdated,
  selectActiveTransactionsCombiner,
);

export const selectActiveTransaction = createSelector(
  selectActiveTransactions,
  selectRouterProps,
  selectActiveTransactionCombiner,
);

export const selectContentionDetails = createSelector(
  selectClusterLocks,
  selectActiveTransactions,
  selectActiveTransaction,
  selectContentionDetailsCombiner,
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
  internalAppNamePrefix: selectAppName(state),
});

// This object is just for convenience so we don't need to supply dispatch to
// each action.
export const activeTransactionsPageActionCreators: ActiveTransactionsViewDispatchProps =
  {
    onColumnsSelect: (columns: string[]) =>
      transactionsColumnsLocalSetting.set(columns.join(",")),
    onFiltersChange: (filters: ActiveTransactionFilters) =>
      filtersLocalSetting.set(filters),
    onSortChange: (ss: SortSetting) => sortSettingLocalSetting.set(ss),
    refreshLiveWorkload,
  };
