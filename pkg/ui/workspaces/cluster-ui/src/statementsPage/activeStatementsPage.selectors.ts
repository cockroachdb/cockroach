// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Dispatch } from "redux";
import { createSelector } from "reselect";
import {
  ActiveStatementFilters,
  ActiveStatementsViewDispatchProps,
  ActiveStatementsViewStateProps,
  AppState,
  SortSetting,
} from "src";
import {
  selectActiveStatements,
  selectAppName,
  selectExecutionStatus,
  selectClusterLocksMaxApiSizeReached,
} from "src/selectors/activeExecutions.selectors";
import { actions as localStorageActions } from "src/store/localStorage";
import { actions as sessionsActions } from "src/store/sessions";
import { selectIsTenant } from "src/store/uiConfig";
import { localStorageSelector } from "../store/utils/selectors";

export const selectSortSetting = (state: AppState): SortSetting =>
  localStorageSelector(state)["sortSetting/ActiveStatementsPage"];

export const selectFilters = (state: AppState): ActiveStatementFilters =>
  localStorageSelector(state)["filters/ActiveStatementsPage"];

export const selectIsAutoRefreshEnabled = (state: AppState): boolean => {
  return localStorageSelector(state)["isAutoRefreshEnabled/ActiveExecutions"];
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
  executionStatus: selectExecutionStatus(),
  internalAppNamePrefix: selectAppName(state),
  isTenant: selectIsTenant(state),
  maxSizeApiReached: selectClusterLocksMaxApiSizeReached(state),
  isAutoRefreshEnabled: selectIsAutoRefreshEnabled(state),
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
        key: "isAutoRefreshEnabled/ActiveExecutions",
        value: isEnabled,
      }),
    );
  },
  onTimestampChange(ts: string) {
    dispatch(
      localStorageActions.update({
        key: "lastTimestampRefresh/ActiveExecutions",
        value: ts,
      }),
    );
  },
});
