// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { Dispatch } from "redux";

import { SortSetting } from "src/sortedtable";
import { AppState, uiConfigActions } from "src/store";
import { selectDropUnusedIndexDuration } from "src/store/clusterSettings/clusterSettings.selectors";
import {
  actions,
  selectSchemaInsights,
  selectSchemaInsightsDatabases,
  selectSchemaInsightsError,
  selectSchemaInsightsMaxApiSizeReached,
  selectSchemaInsightsTypes,
  selectFilters,
  selectSortSetting,
} from "src/store/schemaInsights";

import { actions as analyticsActions } from "../../store/analytics";
import { actions as localStorageActions } from "../../store/localStorage";
import { selectHasAdminRole } from "../../store/uiConfig";
import { SchemaInsightEventFilters } from "../types";

import {
  SchemaInsightsView,
  SchemaInsightsViewDispatchProps,
  SchemaInsightsViewStateProps,
} from "./schemaInsightsView";

const mapStateToProps = (
  state: AppState,
  _props: RouteComponentProps,
): SchemaInsightsViewStateProps => ({
  schemaInsights: selectSchemaInsights(state),
  schemaInsightsDatabases: selectSchemaInsightsDatabases(state),
  schemaInsightsTypes: selectSchemaInsightsTypes(state),
  schemaInsightsError: selectSchemaInsightsError(state),
  filters: selectFilters(state),
  sortSetting: selectSortSetting(state),
  hasAdminRole: selectHasAdminRole(state),
  maxSizeApiReached: selectSchemaInsightsMaxApiSizeReached(state),
  csIndexUnusedDuration: selectDropUnusedIndexDuration(state),
});

const mapDispatchToProps = (
  dispatch: Dispatch,
): SchemaInsightsViewDispatchProps => ({
  onFiltersChange: (filters: SchemaInsightEventFilters) => {
    dispatch(
      localStorageActions.update({
        key: "filters/SchemaInsightsPage",
        value: filters,
      }),
    );
    dispatch(
      analyticsActions.track({
        name: "Filter Clicked",
        page: "Schema Insights",
        filterName: "filters",
        value: filters.toString(),
      }),
    );
  },
  onSortChange: (ss: SortSetting) => {
    dispatch(
      localStorageActions.update({
        key: "sortSetting/SchemaInsightsPage",
        value: ss,
      }),
    );
    dispatch(
      analyticsActions.track({
        name: "Column Sorted",
        page: "Schema Insights",
        tableName: "Schema Insights Table",
        columnName: ss.columnTitle,
      }),
    );
  },
  refreshSchemaInsights: (csIndexUnusedDuration: string) => {
    dispatch(actions.refresh({ csIndexUnusedDuration }));
  },
  refreshUserSQLRoles: () => dispatch(uiConfigActions.refreshUserSQLRoles()),
});

export const SchemaInsightsPageConnected = withRouter(
  connect<
    SchemaInsightsViewStateProps,
    SchemaInsightsViewDispatchProps,
    RouteComponentProps,
    AppState
  >(
    mapStateToProps,
    mapDispatchToProps,
  )(SchemaInsightsView),
);
