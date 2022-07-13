// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { RouteComponentProps } from "react-router-dom";
import { Dispatch } from "redux";
import { createSelector } from "reselect";
import {
  ActiveTransactionFilters,
  ActiveTransactionsViewDispatchProps,
  ActiveTransactionsViewStateProps,
  AppState,
  SortSetting,
} from "src";
import { selectAppName } from "src/statementsPage/activeStatementsPage.selectors";
import { actions as localStorageActions } from "src/store/localStorage";
import { actions as sessionsActions } from "src/store/sessions";
import { localStorageSelector } from "../store/utils/selectors";
import {
  selectActiveTransactionCombiner,
  selectActiveTransactionsCombiner,
  selectContentionDetailsCombiner,
} from "./activeTransactionsCommon.selectors";

const selectSessions = (state: AppState) => state.adminUI.sessions?.data;

const selectSessionsLastUpdated = (state: AppState) =>
  state.adminUI.sessions?.lastUpdated;

export const selectActiveTransactions = createSelector(
  selectSessions,
  selectSessionsLastUpdated,
  selectActiveTransactionsCombiner,
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
});

// ---------------------------------------------------
//       Active transaction details selectors
// ---------------------------------------------------

const selectRouterProps = (_: AppState, props: RouteComponentProps) => props;
const selectClusterLocks = (state: AppState) =>
  state.adminUI.clusterLocks?.data;

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
