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
import {
  refreshTransactionInsights,
  refreshExecutionInsights,
} from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import {
  WorkloadInsightEventFilters,
  SortSetting,
  StatementInsightsViewDispatchProps,
  StatementInsightsViewStateProps,
  TransactionInsightsViewDispatchProps,
  TransactionInsightsViewStateProps,
  WorkloadInsightsRootControl,
  WorkloadInsightsViewProps,
} from "@cockroachlabs/cluster-ui";
import {
  filtersLocalSetting,
  selectStatementInsights,
  sortSettingLocalSetting,
  selectTransactionInsights,
  selectInsightTypes,
} from "src/views/insights/insightsSelectors";
import { bindActionCreators } from "redux";
import { LocalSetting } from "src/redux/localsettings";
import { setGlobalTimeScaleAction } from "src/redux/statements";

export const insightStatementColumnsLocalSetting = new LocalSetting<
  AdminUIState,
  string | null
>(
  "columns/StatementsInsightsPage",
  (state: AdminUIState) => state.localSettings,
  null,
);

const transactionMapStateToProps = (
  state: AdminUIState,
  _props: RouteComponentProps,
): TransactionInsightsViewStateProps => ({
  transactions: selectTransactionInsights(state),
  transactionsError: state.cachedData?.transactionInsights?.lastError,
  insightTypes: selectInsightTypes(),
  filters: filtersLocalSetting.selector(state),
  sortSetting: sortSettingLocalSetting.selector(state),
});

const statementMapStateToProps = (
  state: AdminUIState,
  _props: RouteComponentProps,
): StatementInsightsViewStateProps => ({
  statements: selectStatementInsights(state),
  statementsError: state.cachedData?.executionInsights?.lastError,
  filters: filtersLocalSetting.selector(state),
  insightTypes: selectInsightTypes(),
  sortSetting: sortSettingLocalSetting.selector(state),
  selectedColumnNames:
    insightStatementColumnsLocalSetting.selectorToArray(state),
});

const TransactionDispatchProps = {
  onFiltersChange: (filters: WorkloadInsightEventFilters) =>
    filtersLocalSetting.set(filters),
  onSortChange: (ss: SortSetting) => sortSettingLocalSetting.set(ss),
  setTimeScale: setGlobalTimeScaleAction,
  refreshTransactionInsights: refreshTransactionInsights,
};

const StatementDispatchProps: StatementInsightsViewDispatchProps = {
  onFiltersChange: (filters: WorkloadInsightEventFilters) =>
    filtersLocalSetting.set(filters),
  onSortChange: (ss: SortSetting) => sortSettingLocalSetting.set(ss),
  refreshStatementInsights: refreshExecutionInsights,
  onColumnsChange: (value: string[]) =>
    insightStatementColumnsLocalSetting.set(value.join(",")),
  setTimeScale: setGlobalTimeScaleAction,
};

type StateProps = {
  transactionInsightsViewStateProps: TransactionInsightsViewStateProps;
  statementInsightsViewStateProps: StatementInsightsViewStateProps;
};

type DispatchProps = {
  transactionInsightsViewDispatchProps: TransactionInsightsViewDispatchProps;
  statementInsightsViewDispatchProps: StatementInsightsViewDispatchProps;
};

const WorkloadInsightsPage = withRouter(
  connect<
    StateProps,
    DispatchProps,
    RouteComponentProps,
    WorkloadInsightsViewProps
  >(
    (state: AdminUIState, props: RouteComponentProps) => ({
      transactionInsightsViewStateProps: transactionMapStateToProps(
        state,
        props,
      ),
      statementInsightsViewStateProps: statementMapStateToProps(state, props),
    }),
    dispatch => ({
      transactionInsightsViewDispatchProps: bindActionCreators(
        TransactionDispatchProps,
        dispatch,
      ),
      statementInsightsViewDispatchProps: bindActionCreators(
        StatementDispatchProps,
        dispatch,
      ),
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

export default WorkloadInsightsPage;
