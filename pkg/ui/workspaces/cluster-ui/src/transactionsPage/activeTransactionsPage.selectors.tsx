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
import { getActiveTransactionsFromSessions } from "../activeExecutions/activeStatementUtils";
import { localStorageSelector } from "src/statementsPage/statementsPage.selectors";
import { selectAppName } from "src/statementsPage/activeStatementsPage.selectors";
import {
  ActiveTransactionFilters,
  ActiveTransactionsViewDispatchProps,
  ActiveTransactionsViewStateProps,
  AppState,
  SortSetting,
} from "src";
import { actions as sessionsActions } from "src/store/sessions";
import { actions as localStorageActions } from "src/store/localStorage";
import { Dispatch } from "redux";

export const selectActiveTransactions = createSelector(
  (state: AppState) => state.adminUI.sessions,
  response => {
    if (!response.data) return [];

    return getActiveTransactionsFromSessions(
      response.data,
      response.lastUpdated,
    );
  },
);

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
  sessionsError: state.adminUI.sessions.lastError,
  selectedColumns: selectColumns(state),
  sortSetting: selectSortSetting(state),
  filters: selectFilters(state),
  internalAppNamePrefix: selectAppName(state),
});

export const mapDispatchToActiveTransactionsPageProps = (
  dispatch: Dispatch,
): ActiveTransactionsViewDispatchProps => ({
  refreshSessions: () => dispatch(sessionsActions.refresh()),
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
});
