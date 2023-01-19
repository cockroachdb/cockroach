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
  selectExecutionInsights,
  selectExecutionInsightsError,
  selectExecutionInsightsLoading,
  selectInsightTypes,
} from "src/store/insights/statementInsights";
import {
  actions as transactionInsights,
  selectTransactionInsights,
  selectTransactionInsightsError,
  selectFilters,
  selectSortSetting,
  selectTransactionInsightsLoading,
} from "src/store/insights/transactionInsights";
import { Dispatch } from "redux";
import { TimeScale } from "../../timeScaleDropdown";
import { ExecutionInsightsRequest } from "../../api";
import { selectTimeScale } from "../../store/utils/selectors";

const transactionMapStateToProps = (
  state: AppState,
  _props: RouteComponentProps,
): TransactionInsightsViewStateProps => ({
  transactions: selectTransactionInsights(state),
  transactionsError: selectTransactionInsightsError(state),
  insightTypes: selectInsightTypes(),
  filters: selectFilters(state),
  sortSetting: selectSortSetting(state),
  timeScale: selectTimeScale(state),
  isLoading: selectTransactionInsightsLoading(state),
});

const statementMapStateToProps = (
  state: AppState,
  _props: RouteComponentProps,
): StatementInsightsViewStateProps => ({
  statements: selectExecutionInsights(state),
  statementsError: selectExecutionInsightsError(state),
  insightTypes: selectInsightTypes(),
  filters: selectFilters(state),
  sortSetting: selectSortSetting(state),
  selectedColumnNames: selectColumns(state),
  timeScale: selectTimeScale(state),
  isLoading: selectExecutionInsightsLoading(state),
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
      transactionInsights.updateTimeScale({
        ts: ts,
      }),
    );
  },
  refreshTransactionInsights: (req: ExecutionInsightsRequest) => {
    dispatch(transactionInsights.refresh(req));
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
      statementInsights.updateTimeScale({
        ts: ts,
      }),
    );
  },
  refreshStatementInsights: (req: ExecutionInsightsRequest) => {
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
