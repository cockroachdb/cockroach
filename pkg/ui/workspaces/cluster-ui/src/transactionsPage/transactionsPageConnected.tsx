// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { Dispatch } from "redux";

import { SqlStatsSortType, StatementsRequest } from "src/api/statementsApi";
import { AppState, uiConfigActions } from "src/store";
import { actions as nodesActions } from "src/store/nodes";
import { actions as sqlStatsActions } from "src/store/sqlStats";
import { actions as txnStatsActions } from "src/store/transactionStats";

import { Filters } from "../queryFilter";
import { actions as analyticsActions } from "../store/analytics";
import {
  actions as localStorageActions,
  updateTxnsPageLimitAction,
  updateTxnsPageReqSortAction,
} from "../store/localStorage";
import { nodeRegionsByIDSelector } from "../store/nodes";
import { selectHasAdminRole, selectIsTenant } from "../store/uiConfig";
import {
  selectTxnsPageLimit,
  selectTxnsPageReqSort,
  selectTimeScale,
} from "../store/utils/selectors";
import { TimeScale } from "../timeScaleDropdown";

import {
  mapStateToActiveTransactionsPageProps,
  mapDispatchToActiveTransactionsPageProps,
} from "./activeTransactionsPage.selectors";
import {
  ActiveTransactionsViewStateProps,
  ActiveTransactionsViewDispatchProps,
} from "./activeTransactionsView";
import {
  TransactionsPageStateProps,
  TransactionsPageDispatchProps,
} from "./transactionsPage";
import {
  selectTxnColumns,
  selectSortSetting,
  selectFilters,
  selectSearch,
  selectRequestTime,
} from "./transactionsPage.selectors";
import {
  TransactionsPageRoot,
  TransactionsPageRootProps,
} from "./transactionsPageRoot";

type StateProps = {
  fingerprintsPageProps: TransactionsPageStateProps & RouteComponentProps;
  activePageProps: ActiveTransactionsViewStateProps;
};

type DispatchProps = {
  fingerprintsPageProps: TransactionsPageDispatchProps;
  activePageProps: ActiveTransactionsViewDispatchProps;
};

export const TransactionsPageConnected = withRouter(
  connect<
    StateProps,
    DispatchProps,
    RouteComponentProps,
    TransactionsPageRootProps,
    AppState
  >(
    (state: AppState, props) => ({
      fingerprintsPageProps: {
        ...props,
        columns: selectTxnColumns(state),
        txnsResp: state.adminUI?.transactions,
        timeScale: selectTimeScale(state),
        filters: selectFilters(state),
        isTenant: selectIsTenant(state),
        nodeRegions: nodeRegionsByIDSelector(state),
        search: selectSearch(state),
        sortSetting: selectSortSetting(state),
        hasAdminRole: selectHasAdminRole(state),
        limit: selectTxnsPageLimit(state),
        reqSortSetting: selectTxnsPageReqSort(state),
        requestTime: selectRequestTime(state),
        oldestDataAvailable:
          state.adminUI?.transactions?.data?.oldest_aggregated_ts_returned,
      },
      activePageProps: mapStateToActiveTransactionsPageProps(state),
    }),
    (dispatch: Dispatch) => ({
      fingerprintsPageProps: {
        refreshData: (req: StatementsRequest) =>
          dispatch(txnStatsActions.refresh(req)),
        refreshNodes: () => dispatch(nodesActions.refresh()),
        refreshUserSQLRoles: () =>
          dispatch(uiConfigActions.refreshUserSQLRoles()),
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
              filterName: "filters",
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
        onChangeLimit: (limit: number) =>
          dispatch(updateTxnsPageLimitAction(limit)),
        onChangeReqSort: (sort: SqlStatsSortType) =>
          dispatch(updateTxnsPageReqSortAction(sort)),
        onApplySearchCriteria: (ts: TimeScale, limit: number, sort: string) =>
          dispatch(
            analyticsActions.track({
              name: "Apply Search Criteria",
              page: "Transactions",
              tsValue: ts.key,
              limitValue: limit,
              sortValue: sort,
            }),
          ),
        onRequestTimeChange: (t: moment.Moment) => {
          dispatch(
            localStorageActions.update({
              key: "requestTime/StatementsPage",
              value: t,
            }),
          );
        },
      },
      activePageProps: mapDispatchToActiveTransactionsPageProps(dispatch),
    }),
    (stateProps, dispatchProps) => ({
      fingerprintsPageProps: {
        ...stateProps.fingerprintsPageProps,
        ...dispatchProps.fingerprintsPageProps,
      },
      activePageProps: {
        ...stateProps.activePageProps,
        ...dispatchProps.activePageProps,
      },
    }),
  )(TransactionsPageRoot),
);
