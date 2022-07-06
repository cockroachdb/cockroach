// Copyright 2018 The Cockroach Authors.
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
import { refreshContentionTransactions } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import {
  InsightsTransactionFilters,
  SortSetting,
  InsightsTransactionsViewDispatchProps,
  InsightsTransactionsViewStateProps,
  InsightsTransactionsView,
} from "@cockroachlabs/cluster-ui";
import { selectAppName } from "src/views/statements/activeStatementsSelectors";
import {
  filtersLocalSetting,
  sortSettingLocalSetting,
  selectContentionTransactionEvents,
} from "src/views/insights/redux";

const mapStateToProps = (
  state: AdminUIState,
): InsightsTransactionsViewStateProps => ({
  contentionTransactions: selectContentionTransactionEvents(state),
  contentionTransactionsError:
    state.cachedData?.contentionTransactions.lastError,
  filters: filtersLocalSetting.selector(state),
  sortSetting: sortSettingLocalSetting.selector(state),
  internalAppNamePrefix: selectAppName(state),
});

const mapDispatchToProps = {
  onFiltersChange: (filters: InsightsTransactionFilters) =>
    filtersLocalSetting.set(filters),
  onSortChange: (ss: SortSetting) => sortSettingLocalSetting.set(ss),
  refreshContentionTransactions: refreshContentionTransactions,
};

const InsightsTransactionsPageConnected = withRouter(
  connect<
    InsightsTransactionsViewStateProps,
    InsightsTransactionsViewDispatchProps,
    RouteComponentProps
  >(
    mapStateToProps,
    mapDispatchToProps,
  )(InsightsTransactionsView),
);

export default InsightsTransactionsPageConnected;
