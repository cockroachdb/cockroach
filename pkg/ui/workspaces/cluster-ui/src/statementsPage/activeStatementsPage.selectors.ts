// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Dispatch } from "redux";
import { createSelector } from "reselect";
import {
  ActiveStatementFilters,
  ActiveStatementsViewDispatchProps,
  ActiveStatementsViewStateProps,
  AppState,
  SortSetting,
  analyticsActions,
} from "src";

import {
  selectActiveStatements,
  selectAppName,
  selectClusterLocksMaxApiSizeReached,
} from "src/selectors/activeExecutions.selectors";
import {
  LocalStorageKeys,
  actions as localStorageActions,
} from "src/store/localStorage";
import { actions as sessionsActions } from "src/store/sessions";
import { selectIsTenant } from "src/store/uiConfig";

import { localStorageSelector } from "../store/utils/selectors";

export const selectSortSetting = (state: AppState): SortSetting =>
  localStorageSelector(state)["sortSetting/ActiveStatementsPage"];

export const selectFilters = (state: AppState): ActiveStatementFilters =>
  localStorageSelector(state)["filters/ActiveStatementsPage"];

export const selectIsAutoRefreshEnabled = (state: AppState): boolean => {
  return localStorageSelector(state)[
    LocalStorageKeys.ACTIVE_EXECUTIONS_IS_AUTOREFRESH_ENABLED
  ];
};

const selectLocalStorageColumns = (state: AppState) => {
  const localStorage = localStorageSelector(state);
  return localStorage["showColumns/ActiveStatementsPage"];
};

export const selectColumns = createSelector(
  selectLocalStorageColumns,
  value => {
    if (value == null) return null;

    return value.split(",").map(col => col.trim());
  },
);

export const mapStateToActiveStatementsPageProps = (
  state: AppState,
): ActiveStatementsViewStateProps => ({
  statements: selectActiveStatements(state),
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

export const mapDispatchToActiveStatementsPageProps = (
  dispatch: Dispatch,
): ActiveStatementsViewDispatchProps => ({
  refreshLiveWorkload: () => dispatch(sessionsActions.refresh()),
  onColumnsSelect: columns => {
    dispatch(
      localStorageActions.update({
        key: "showColumns/ActiveStatementsPage",
        value: columns.join(","),
      }),
    );
  },
  onFiltersChange: (filters: ActiveStatementFilters) =>
    dispatch(
      localStorageActions.update({
        key: "filters/ActiveStatementsPage",
        value: filters,
      }),
    ),
  onSortChange: (ss: SortSetting) =>
    dispatch(
      localStorageActions.update({
        key: "sortSetting/ActiveStatementsPage",
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
        page: "Statements",
        value: isEnabled,
      }),
    );
  },
  onManualRefresh: () => {
    dispatch(sessionsActions.refresh());
    dispatch(
      analyticsActions.track({
        name: "Manual Refresh",
        page: "Statements",
      }),
    );
  },
});
