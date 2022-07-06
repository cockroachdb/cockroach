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
import { refreshInsights } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import {
  InsightEventFilters,
  SortSetting,
  TransactionInsightsViewStateProps,
  TransactionInsightsViewDispatchProps,
  TransactionInsightsView,
} from "@cockroachlabs/cluster-ui";
import { selectAppName } from "src/views/statements/activeStatementsSelectors";
import {
  filtersLocalSetting,
  sortSettingLocalSetting,
  selectInsights,
} from "src/views/insights/redux";

const mapStateToProps = (
  state: AdminUIState,
  _props: RouteComponentProps,
): TransactionInsightsViewStateProps => ({
  transactions: selectInsights(state),
  transactionsError: state.cachedData?.insights.lastError,
  filters: filtersLocalSetting.selector(state),
  sortSetting: sortSettingLocalSetting.selector(state),
  internalAppNamePrefix: selectAppName(state),
});

const mapDispatchToProps = {
  onFiltersChange: (filters: InsightEventFilters) =>
    filtersLocalSetting.set(filters),
  onSortChange: (ss: SortSetting) => sortSettingLocalSetting.set(ss),
  refreshTransactionInsights: refreshInsights,
};

const WorkloadInsightsPageConnected = withRouter(
  connect<
    TransactionInsightsViewStateProps,
    TransactionInsightsViewDispatchProps,
    RouteComponentProps
  >(
    mapStateToProps,
    mapDispatchToProps,
  )(TransactionInsightsView),
);

export default WorkloadInsightsPageConnected;
