// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
import { selectDropUnusedIndexDuration } from "src/redux/clusterSettings";

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
  csIndexUnusedDuration: selectDropUnusedIndexDuration(state),
});

const mapDispatchToProps = {
  onFiltersChange: (filters: SchemaInsightEventFilters) =>
    schemaInsightsFiltersLocalSetting.set(filters),
  onSortChange: (ss: SortSetting) => schemaInsightsSortLocalSetting.set(ss),
  refreshSchemaInsights: (csIndexUnusedDuration: string) => {
    return refreshSchemaInsights({ csIndexUnusedDuration });
  },
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
