// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Dispatch } from "redux";
import { createSelector } from "reselect";
import {
  ActiveTransactionFilters,
  ActiveTransactionsViewDispatchProps,
  ActiveTransactionsViewStateProps,
  AppState,
  SortSetting,
  analyticsActions,
} from "src";

import {
  selectAppName,
  selectActiveTransactions,
  selectClusterLocksMaxApiSizeReached,
} from "src/selectors/activeExecutions.selectors";
import { selectIsAutoRefreshEnabled } from "src/statementsPage/activeStatementsPage.selectors";
import {
  LocalStorageKeys,
  actions as localStorageActions,
} from "src/store/localStorage";
import { actions as sessionsActions } from "src/store/sessions";
import { selectIsTenant } from "src/store/uiConfig";

import { localStorageSelector } from "../store/utils/selectors";

export const selectSortSetting = (state: AppState): SortSetting =>
  localStorageSelector(state)["sortSetting/ActiveTransactionsPage"];

export const selectFilters = (state: AppState): ActiveTransactionFilters =>
  localStorageSelector(state)["filters/ActiveTransactionsPage"];

const selectLocalStorageColumns = (state: AppState) => {
  const localStorage = localStorageSelector(state);
  return localStorage["showColumns/ActiveTransactionsPage"];
};

export const selectColumns = createSelector(
  selectLocalStorageColumns,
  value => {
    if (value == null) return null;

    return value.split(",").map(col => col.trim());
  },
);

export const mapStateToActiveTransactionsPageProps = (
  state: AppState,
): ActiveTransactionsViewStateProps => ({
  transactions: selectActiveTransactions(state),
  sessionsError: state.adminUI?.sessions.lastError,
  selectedColumns: selectColumns(state),
  sortSetting: selectSortSetting(state),
  filters: selectFilters(state),
  internalAppNamePrefix: selectAppName(state),
  isTenant: selectIsTenant(state),
  maxSizeApiReached: selectClusterLocksMaxApiSizeReached(state),
  isAutoRefreshEnabled: selectIsAutoRefreshEnabled(state),
  lastUpdated: state.adminUI?.sessions.lastUpdated,
});

export const mapDispatchToActiveTransactionsPageProps = (
  dispatch: Dispatch,
): ActiveTransactionsViewDispatchProps => ({
  refreshLiveWorkload: () => dispatch(sessionsActions.refresh()),
  onColumnsSelect: columns =>
    dispatch(
      localStorageActions.update({
        key: "showColumns/ActiveTransactionsPage",
        value: columns ? columns.join(", ") : " ",
      }),
    ),
  onFiltersChange: (filters: ActiveTransactionFilters) =>
    dispatch(
      localStorageActions.update({
        key: "filters/ActiveTransactionsPage",
        value: filters,
      }),
    ),
  onSortChange: (ss: SortSetting) =>
    dispatch(
      localStorageActions.update({
        key: "sortSetting/ActiveTransactionsPage",
        value: ss,
      }),
    ),
  onAutoRefreshToggle: (isEnabled: boolean) => {
    dispatch(
      localStorageActions.update({
        key: LocalStorageKeys.ACTIVE_EXECUTIONS_IS_AUTOREFRESH_ENABLED,
        value: isEnabled,
      }),
    );
    dispatch(
      analyticsActions.track({
        name: "Auto Refresh Toggle",
        page: "Transactions",
        value: isEnabled,
      }),
    );
  },
  onManualRefresh: () => {
    dispatch(sessionsActions.refresh());
    dispatch(
      analyticsActions.track({
        name: "Manual Refresh",
        page: "Transactions",
      }),
    );
  },
});
