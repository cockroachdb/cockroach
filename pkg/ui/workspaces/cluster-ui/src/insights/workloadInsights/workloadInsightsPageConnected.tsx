// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { Dispatch } from "redux";

import { StmtInsightsReq, TxnInsightsRequest } from "src/api";
import { selectStmtInsights } from "src/selectors/common";
import { SortSetting } from "src/sortedtable";
import { AppState } from "src/store";
import {
  actions as statementInsights,
  selectColumns,
  selectInsightTypes,
  selectStmtInsightsError,
  selectStmtInsightsLoading,
  selectStmtInsightsMaxApiReached,
} from "src/store/insights/statementInsights";
import {
  actions as txnInsights,
  selectTransactionInsights,
  selectTransactionInsightsError,
  selectFilters,
  selectSortSetting,
  selectTransactionInsightsLoading,
  selectTransactionInsightsMaxApiReached,
} from "src/store/insights/transactionInsights";
import { actions as localStorageActions } from "src/store/localStorage";
import { actions as sqlActions } from "src/store/sqlStats";

import { actions as analyticsActions } from "../../store/analytics";
import { selectTimeScale } from "../../store/utils/selectors";
import { TimeScale } from "../../timeScaleDropdown";
import { WorkloadInsightEventFilters } from "../types";

import {
  StatementInsightsViewDispatchProps,
  StatementInsightsViewStateProps,
} from "./statementInsights";
import {
  TransactionInsightsViewDispatchProps,
  TransactionInsightsViewStateProps,
} from "./transactionInsights";
import {
  WorkloadInsightsViewProps,
  WorkloadInsightsRootControl,
} from "./workloadInsightRootControl";

const transactionMapStateToProps = (
  state: AppState,
  _props: RouteComponentProps,
): TransactionInsightsViewStateProps => ({
  isDataValid: state.adminUI?.txnInsights?.valid,
  lastUpdated: state.adminUI?.txnInsights.lastUpdated,
  transactions: selectTransactionInsights(state),
  transactionsError: selectTransactionInsightsError(state),
  insightTypes: selectInsightTypes(),
  filters: selectFilters(state),
  sortSetting: selectSortSetting(state),
  timeScale: selectTimeScale(state),
  isLoading: selectTransactionInsightsLoading(state),
  maxSizeApiReached: selectTransactionInsightsMaxApiReached(state),
});

const statementMapStateToProps = (
  state: AppState,
  _props: RouteComponentProps,
): StatementInsightsViewStateProps => ({
  isDataValid: state.adminUI?.stmtInsights?.valid,
  lastUpdated: state.adminUI?.stmtInsights.lastUpdated,
  statements: selectStmtInsights(state),
  statementsError: selectStmtInsightsError(state),
  insightTypes: selectInsightTypes(),
  filters: selectFilters(state),
  sortSetting: selectSortSetting(state),
  selectedColumnNames: selectColumns(state),
  timeScale: selectTimeScale(state),
  isLoading: selectStmtInsightsLoading(state),
  maxSizeApiReached: selectStmtInsightsMaxApiReached(state),
});

const TransactionDispatchProps = (
  dispatch: Dispatch,
): TransactionInsightsViewDispatchProps => ({
  onFiltersChange: (filters: WorkloadInsightEventFilters) => {
    dispatch(
      localStorageActions.update({
        key: "filters/InsightsPage",
        value: filters,
      }),
    );
    dispatch(
      analyticsActions.track({
        name: "Filter Clicked",
        page: "Workload Insights - Transaction",
        filterName: "filters",
        value: filters.toString(),
      }),
    );
  },
  onSortChange: (ss: SortSetting) => {
    dispatch(
      localStorageActions.update({
        key: "sortSetting/InsightsPage",
        value: ss,
      }),
    );
    dispatch(
      analyticsActions.track({
        name: "Column Sorted",
        page: "Workload Insights - Transaction",
        tableName: "Workload Transaction Insights Table",
        columnName: ss.columnTitle,
      }),
    );
  },
  setTimeScale: (ts: TimeScale) => {
    dispatch(
      sqlActions.updateTimeScale({
        ts: ts,
      }),
    );
    dispatch(
      analyticsActions.track({
        name: "TimeScale changed",
        page: "Workload Insights - Transaction",
        value: ts.key,
      }),
    );
  },
  refreshTransactionInsights: (req: TxnInsightsRequest) => {
    dispatch(txnInsights.refresh(req));
  },
});

const StatementDispatchProps = (
  dispatch: Dispatch,
): StatementInsightsViewDispatchProps => ({
  onFiltersChange: (filters: WorkloadInsightEventFilters) => {
    dispatch(
      localStorageActions.update({
        key: "filters/InsightsPage",
        value: filters,
      }),
    );
    dispatch(
      analyticsActions.track({
        name: "Filter Clicked",
        page: "Workload Insights - Statement",
        filterName: "filters",
        value: filters.toString(),
      }),
    );
  },
  onSortChange: (ss: SortSetting) => {
    dispatch(
      localStorageActions.update({
        key: "sortSetting/InsightsPage",
        value: ss,
      }),
    );
    dispatch(
      analyticsActions.track({
        name: "Column Sorted",
        page: "Workload Insights - Statement",
        tableName: "Workload Statement Insights Table",
        columnName: ss.columnTitle,
      }),
    );
  },
  // We use `null` when the value was never set and it will show all columns.
  // If the user modifies the selection and no columns are selected,
  // the function will save the value as a blank space, otherwise
  // it gets saved as `null`.
  onColumnsChange: (value: string[]) => {
    const columns = value.length === 0 ? " " : value.join(",");
    dispatch(
      localStorageActions.update({
        key: "showColumns/StatementInsightsPage",
        value: columns,
      }),
    );
    dispatch(
      analyticsActions.track({
        name: "Columns Selected change",
        page: "Workload Insights - Statement",
        value: columns,
      }),
    );
  },
  setTimeScale: (ts: TimeScale) => {
    dispatch(
      sqlActions.updateTimeScale({
        ts: ts,
      }),
    );
    dispatch(
      analyticsActions.track({
        name: "TimeScale changed",
        page: "Workload Insights - Statement",
        value: ts.key,
      }),
    );
  },
  refreshStatementInsights: (req: StmtInsightsReq) => {
    dispatch(statementInsights.refresh(req));
  },
});

type StateProps = {
  transactionInsightsViewStateProps: TransactionInsightsViewStateProps;
  statementInsightsViewStateProps: StatementInsightsViewStateProps;
};

type DispatchProps = {
  transactionInsightsViewDispatchProps: TransactionInsightsViewDispatchProps;
  statementInsightsViewDispatchProps: StatementInsightsViewDispatchProps;
};

export const WorkloadInsightsPageConnected = withRouter(
  connect<
    StateProps,
    DispatchProps,
    RouteComponentProps,
    WorkloadInsightsViewProps,
    AppState
  >(
    (state: AppState, props: RouteComponentProps) => ({
      transactionInsightsViewStateProps: transactionMapStateToProps(
        state,
        props,
      ),
      statementInsightsViewStateProps: statementMapStateToProps(state, props),
    }),
    dispatch => ({
      transactionInsightsViewDispatchProps: TransactionDispatchProps(dispatch),
      statementInsightsViewDispatchProps: StatementDispatchProps(dispatch),
    }),
    (stateProps, dispatchProps) => ({
      transactionInsightsViewProps: {
        ...stateProps.transactionInsightsViewStateProps,
        ...dispatchProps.transactionInsightsViewDispatchProps,
      },
      statementInsightsViewProps: {
        ...stateProps.statementInsightsViewStateProps,
        ...dispatchProps.statementInsightsViewDispatchProps,
      },
    }),
  )(WorkloadInsightsRootControl),
);
