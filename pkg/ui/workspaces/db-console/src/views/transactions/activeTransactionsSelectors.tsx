// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  ActiveTransactionFilters,
  ActiveTransactionsViewDispatchProps,
  defaultFilters,
  SortSetting,
} from "@cockroachlabs/cluster-ui";

import { refreshLiveWorkload } from "src/redux/apiReducers";
import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";
import {
  selectAppName,
  selectActiveTransactions,
  selectClusterLocksMaxApiSizeReached,
} from "src/selectors";

export const ACTIVE_EXECUTIONS_IS_AUTOREFRESH_ENABLED =
  "isAutoRefreshEnabled/ActiveExecutions";
export const ACTIVE_EXECUTIONS_DISPLAY_REFRESH_ALERT =
  "displayRefreshAlert/ActiveTransactionsPage";

const transactionsColumnsLocalSetting = new LocalSetting<
  AdminUIState,
  string | null
>(
  "columns/ActiveTransactionsPage",
  (state: AdminUIState) => state.localSettings,
  null,
);

const defaultActiveTxnFilters = {
  app: defaultFilters.app,
  executionStatus: defaultFilters.executionStatus,
};

const filtersLocalSetting = new LocalSetting<
  AdminUIState,
  ActiveTransactionFilters
>(
  "filters/ActiveTransactionsPage",
  (state: AdminUIState) => state.localSettings,
  defaultActiveTxnFilters,
);

const sortSettingLocalSetting = new LocalSetting<AdminUIState, SortSetting>(
  "sortSetting/ActiveTransactionsPage",
  (state: AdminUIState) => state.localSettings,
  { ascending: false, columnTitle: "startTime" },
);

// autoRefreshLocalSetting is shared between the Active Statements and Active
// Transactions components.
export const autoRefreshLocalSetting = new LocalSetting<AdminUIState, boolean>(
  ACTIVE_EXECUTIONS_IS_AUTOREFRESH_ENABLED,
  (state: AdminUIState) => state.localSettings,
  true,
);

export const mapStateToActiveTransactionsPageProps = (state: AdminUIState) => ({
  selectedColumns: transactionsColumnsLocalSetting.selectorToArray(state),
  transactions: selectActiveTransactions(state),
  sessionsError: state.cachedData?.sessions.lastError,
  filters: filtersLocalSetting.selector(state),
  sortSetting: sortSettingLocalSetting.selector(state),
  internalAppNamePrefix: selectAppName(state),
  maxSizeApiReached: selectClusterLocksMaxApiSizeReached(state),
  isAutoRefreshEnabled: autoRefreshLocalSetting.selector(state),
  lastUpdated: state.cachedData?.sessions.setAt,
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
    onAutoRefreshToggle: (isToggled: boolean) =>
      autoRefreshLocalSetting.set(isToggled),
    onManualRefresh: () => refreshLiveWorkload(),
  };
