// Copyright 2022 The Cockroach Authors.
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
import { AppState } from "src/store";
import { actions as localStorageActions } from "src/store/localStorage";
import {
  TransactionInsightsViewDispatchProps,
  TransactionInsightsViewStateProps,
} from "./transactionInsights";
import {
  StatementInsightsViewDispatchProps,
  StatementInsightsViewStateProps,
} from "./statementInsights";
import { WorkloadInsightEventFilters } from "../types";
import {
  WorkloadInsightsViewProps,
  WorkloadInsightsRootControl,
} from "./workloadInsightRootControl";
import { SortSetting } from "src/sortedtable";
import {
  actions as statementInsights,
  selectColumns,
  selectStatementInsights,
  selectStatementInsightsError,
} from "src/store/insights/statementInsights";
import {
  actions as transactionInsights,
  selectTransactionInsights,
  selectTransactionInsightsError,
  selectFilters,
  selectSortSetting,
} from "src/store/insights/transactionInsights";
import { bindActionCreators, Dispatch } from "redux";
import { TimeScale } from "../../timeScaleDropdown";
import { actions as sqlStatsActions } from "../../store/sqlStats";
import {
  StatementInsightDetails,
  StatementInsightDetailsDispatchProps,
  StatementInsightDetailsStateProps,
} from "../workloadInsightDetails";

const transactionMapStateToProps = (
  state: AppState,
  _props: RouteComponentProps,
): TransactionInsightsViewStateProps => ({
  transactions: selectTransactionInsights(state),
  transactionsError: selectTransactionInsightsError(state),
  filters: selectFilters(state),
  sortSetting: selectSortSetting(state),
});

const statementMapStateToProps = (
  state: AppState,
  _props: RouteComponentProps,
): StatementInsightsViewStateProps => ({
  statements: selectStatementInsights(state),
  statementsError: selectStatementInsightsError(state),
  filters: selectFilters(state),
  sortSetting: selectSortSetting(state),
  selectedColumnNames: selectColumns(state),
});

const TransactionDispatchProps = (
  dispatch: Dispatch,
): TransactionInsightsViewDispatchProps => ({
  onFiltersChange: (filters: WorkloadInsightEventFilters) =>
    localStorageActions.update({
      key: "filters/InsightsPage",
      value: filters,
    }),
  onSortChange: (ss: SortSetting) =>
    localStorageActions.update({
      key: "sortSetting/InsightsPage",
      value: ss,
    }),
  setTimeScale: (ts: TimeScale) => {
    dispatch(
      sqlStatsActions.updateTimeScale({
        ts: ts,
      }),
    );
  },
  refreshTransactionInsights: transactionInsights.refresh,
});

const StatementDispatchProps = (
  dispatch: Dispatch,
): StatementInsightsViewDispatchProps => ({
  onFiltersChange: (filters: WorkloadInsightEventFilters) =>
    localStorageActions.update({
      key: "filters/InsightsPage",
      value: filters,
    }),
  onSortChange: (ss: SortSetting) =>
    localStorageActions.update({
      key: "sortSetting/InsightsPage",
      value: ss,
    }),
  onColumnsChange: (value: string[]) =>
    localStorageActions.update({
      key: "showColumns/StatementInsightsPage",
      value: value.join(","),
    }),
  setTimeScale: (ts: TimeScale) => {
    dispatch(
      sqlStatsActions.updateTimeScale({
        ts: ts,
      }),
    );
  },
  refreshStatementInsights: statementInsights.refresh,
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
    WorkloadInsightsViewProps
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
