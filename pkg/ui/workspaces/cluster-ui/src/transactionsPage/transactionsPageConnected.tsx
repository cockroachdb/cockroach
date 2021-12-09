// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { Dispatch } from "redux";

import { AppState } from "src/store";
import { actions as sqlStatsActions } from "src/store/sqlStats";
import { TransactionsPage } from "./transactionsPage";
import {
  TransactionsPageStateProps,
  TransactionsPageDispatchProps,
} from "./transactionsPage";
import {
  selectTransactionsData,
  selectTransactionsLastError,
  selectTxnColumns,
  selectSortSetting,
  selectFilters,
  selectSearch,
} from "./transactionsPage.selectors";
import { selectIsTenant } from "../store/uiConfig";
import { nodeRegionsByIDSelector } from "../store/nodes";
import { selectTimeScale } from "src/statementsPage/statementsPage.selectors";
import { StatementsRequest } from "src/api/statementsApi";
import { actions as localStorageActions } from "../store/localStorage";
import { Filters } from "../queryFilter";
import { actions as analyticsActions } from "../store/analytics";
import { TimeScale } from "../timeScaleDropdown";

export const TransactionsPageConnected = withRouter(
  connect<
    TransactionsPageStateProps,
    TransactionsPageDispatchProps,
    RouteComponentProps
  >(
    (state: AppState) => ({
      columns: selectTxnColumns(state),
      data: selectTransactionsData(state),
      timeScale: selectTimeScale(state),
      error: selectTransactionsLastError(state),
      filters: selectFilters(state),
      isTenant: selectIsTenant(state),
      nodeRegions: nodeRegionsByIDSelector(state),
      search: selectSearch(state),
      sortSetting: selectSortSetting(state),
    }),
    (dispatch: Dispatch) => ({
      refreshData: (req?: StatementsRequest) =>
        dispatch(sqlStatsActions.refresh(req)),
      resetSQLStats: () => dispatch(sqlStatsActions.reset()),
      onTimeScaleChange: (ts: TimeScale) => {
        dispatch(
          sqlStatsActions.updateTimeScale({
            ts: ts,
          }),
        );
      },
      // We use `null` when the value was never set and it will show all columns.
      // If the user modifies the selection and no columns are selected,
      // the function will save the value as a blank space, otherwise
      // it gets saved as `null`.
      onColumnsChange: (selectedColumns: string[]) =>
        dispatch(
          localStorageActions.update({
            key: "showColumns/TransactionPage",
            value:
              selectedColumns.length === 0 ? " " : selectedColumns.join(","),
          }),
        ),
      onSortingChange: (
        tableName: string,
        columnName: string,
        ascending: boolean,
      ) => {
        dispatch(
          localStorageActions.update({
            key: "sortSetting/TransactionsPage",
            value: { columnTitle: columnName, ascending: ascending },
          }),
        );
      },
      onFilterChange: (value: Filters) => {
        dispatch(
          analyticsActions.track({
            name: "Filter Clicked",
            page: "Transactions",
            filterName: "app",
            value: value.toString(),
          }),
        );
        dispatch(
          localStorageActions.update({
            key: "filters/TransactionsPage",
            value: value,
          }),
        );
      },
      onSearchComplete: (query: string) => {
        dispatch(
          analyticsActions.track({
            name: "Keyword Searched",
            page: "Transactions",
          }),
        );
        dispatch(
          localStorageActions.update({
            key: "search/TransactionsPage",
            value: query,
          }),
        );
      },
    }),
  )(TransactionsPage),
);
