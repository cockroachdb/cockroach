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
  refreshSchemaInsights,
  refreshUserSQLRoles,
} from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import {
  SchemaInsightEventFilters,
  SchemaInsightsView,
  SchemaInsightsViewDispatchProps,
  SchemaInsightsViewStateProps,
  SortSetting,
} from "@cockroachlabs/cluster-ui";
import {
  schemaInsightsFiltersLocalSetting,
  schemaInsightsSortLocalSetting,
  selectSchemaInsights,
  selectSchemaInsightsDatabases,
  selectSchemaInsightsMaxApiReached,
  selectSchemaInsightsTypes,
} from "src/views/insights/insightsSelectors";
import { selectHasAdminRole } from "src/redux/user";

const mapStateToProps = (
  state: AdminUIState,
  _props: RouteComponentProps,
): SchemaInsightsViewStateProps => ({
  schemaInsights: selectSchemaInsights(state),
  schemaInsightsDatabases: selectSchemaInsightsDatabases(state),
  schemaInsightsTypes: selectSchemaInsightsTypes(state),
  schemaInsightsError: state.cachedData?.schemaInsights.lastError,
  filters: schemaInsightsFiltersLocalSetting.selector(state),
  sortSetting: schemaInsightsSortLocalSetting.selector(state),
  hasAdminRole: selectHasAdminRole(state),
  maxSizeApiReached: selectSchemaInsightsMaxApiReached(state),
});

const mapDispatchToProps = {
  onFiltersChange: (filters: SchemaInsightEventFilters) =>
    schemaInsightsFiltersLocalSetting.set(filters),
  onSortChange: (ss: SortSetting) => schemaInsightsSortLocalSetting.set(ss),
  refreshSchemaInsights: refreshSchemaInsights,
  refreshUserSQLRoles: refreshUserSQLRoles,
};

const SchemaInsightsPage = withRouter(
  connect<
    SchemaInsightsViewStateProps,
    SchemaInsightsViewDispatchProps,
    RouteComponentProps
  >(
    mapStateToProps,
    mapDispatchToProps,
  )(SchemaInsightsView),
);

export default SchemaInsightsPage;
