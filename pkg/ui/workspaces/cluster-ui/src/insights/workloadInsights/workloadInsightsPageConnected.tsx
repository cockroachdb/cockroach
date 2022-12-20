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
  selectInsightTypes,
} from "src/store/insights/statementInsights";
import {
  actions as transactionInsights,
  selectTransactionInsights,
  selectTransactionInsightsError,
  selectFilters,
  selectSortSetting,
} from "src/store/insights/transactionInsights";
import { Dispatch } from "redux";
import { TimeScale } from "../../timeScaleDropdown";
import { actions as sqlStatsActions } from "../../store/sqlStats";

const transactionMapStateToProps = (
  state: AppState,
  _props: RouteComponentProps,
): TransactionInsightsViewStateProps => ({
  transactions: selectTransactionInsights(state),
  transactionsError: selectTransactionInsightsError(state),
  insightTypes: selectInsightTypes(),
  filters: selectFilters(state),
  sortSetting: selectSortSetting(state),
});

const statementMapStateToProps = (
  state: AppState,
  _props: RouteComponentProps,
): StatementInsightsViewStateProps => ({
  statements: selectStatementInsights(state),
  statementsError: selectStatementInsightsError(state),
  insightTypes: selectInsightTypes(),
  filters: selectFilters(state),
  sortSetting: selectSortSetting(state),
  selectedColumnNames: selectColumns(state),
});

const TransactionDispatchProps = (
  dispatch: Dispatch,
): TransactionInsightsViewDispatchProps => ({
  onFiltersChange: (filters: WorkloadInsightEventFilters) =>
    dispatch(
      localStorageActions.update({
        key: "filters/InsightsPage",
        value: filters,
      }),
    ),
  onSortChange: (ss: SortSetting) =>
    dispatch(
      localStorageActions.update({
        key: "sortSetting/InsightsPage",
        value: ss,
      }),
    ),
  setTimeScale: (ts: TimeScale) => {
    dispatch(
      sqlStatsActions.updateTimeScale({
        ts: ts,
      }),
    );
  },
  refreshTransactionInsights: () => {
    dispatch(transactionInsights.refresh());
  },
});

const StatementDispatchProps = (
  dispatch: Dispatch,
): StatementInsightsViewDispatchProps => ({
  onFiltersChange: (filters: WorkloadInsightEventFilters) =>
    dispatch(
      localStorageActions.update({
        key: "filters/InsightsPage",
        value: filters,
      }),
    ),
  onSortChange: (ss: SortSetting) =>
    dispatch(
      localStorageActions.update({
        key: "sortSetting/InsightsPage",
        value: ss,
      }),
    ),
  // We use `null` when the value was never set and it will show all columns.
  // If the user modifies the selection and no columns are selected,
  // the function will save the value as a blank space, otherwise
  // it gets saved as `null`.
  onColumnsChange: (value: string[]) =>
    dispatch(
      localStorageActions.update({
        key: "showColumns/StatementInsightsPage",
        value: value.length === 0 ? " " : value.join(","),
      }),
    ),
  setTimeScale: (ts: TimeScale) => {
    dispatch(
      sqlStatsActions.updateTimeScale({
        ts: ts,
      }),
    );
  },
  refreshStatementInsights: () => {
    dispatch(statementInsights.refresh());
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
