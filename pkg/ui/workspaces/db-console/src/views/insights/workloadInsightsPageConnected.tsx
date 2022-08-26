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
  refreshInsights,
  refreshStatementInsights,
} from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import {
  InsightEventFilters,
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
} from "src/views/insights/insightsSelectors";
import { bindActionCreators } from "redux";

const mapStateToProps = (
  state: AdminUIState,
  _props: RouteComponentProps,
): TransactionInsightsViewStateProps => ({
  transactions: selectTransactionInsights(state),
  transactionsError: state.cachedData?.insights.lastError,
  filters: filtersLocalSetting.selector(state),
  sortSetting: sortSettingLocalSetting.selector(state),
});

const statementMapStateToProps = (
  state: AdminUIState,
  _props: RouteComponentProps,
): StatementInsightsViewStateProps => ({
  statements: selectStatementInsights(state),
  statementsError: state.cachedData?.insights.lastError,
  filters: filtersLocalSetting.selector(state),
  sortSetting: sortSettingLocalSetting.selector(state),
});

const DispatchProps = {
  onFiltersChange: (filters: InsightEventFilters) =>
    filtersLocalSetting.set(filters),
  onSortChange: (ss: SortSetting) => sortSettingLocalSetting.set(ss),
  refreshTransactionInsights: refreshInsights,
};

const StatementDispatchProps: StatementInsightsViewDispatchProps = {
  onFiltersChange: (filters: InsightEventFilters) =>
    filtersLocalSetting.set(filters),
  onSortChange: (ss: SortSetting) => sortSettingLocalSetting.set(ss),
  refreshStatementInsights: refreshStatementInsights,
};

type StateProps = {
  transactionInsightsViewStateProps: TransactionInsightsViewStateProps;
  statementInsightsViewStateProps: StatementInsightsViewStateProps;
};

type DispatchProps = {
  transactionInsightsViewDispatchProps: TransactionInsightsViewDispatchProps;
  statementInsightsViewDispatchProps: StatementInsightsViewDispatchProps;
};

const WorkloadInsightsPageConnected = withRouter(
  connect<
    StateProps,
    DispatchProps,
    RouteComponentProps,
    WorkloadInsightsViewProps
  >(
    (state: AdminUIState, props: RouteComponentProps) => ({
      transactionInsightsViewStateProps: mapStateToProps(state, props),
      statementInsightsViewStateProps: statementMapStateToProps(state, props),
    }),
    dispatch => ({
      transactionInsightsViewDispatchProps: bindActionCreators(
        DispatchProps,
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

export default WorkloadInsightsPageConnected;
