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
  actions,
  selectSchemaInsights,
  selectSchemaInsightsDatabases,
  selectSchemaInsightsError,
  selectSchemaInsightsTypes,
  selectFilters,
  selectSortSetting,
} from "src/store/schemaInsights";
import { AppState } from "src/store";
import {
  SchemaInsightsView,
  SchemaInsightsViewDispatchProps,
  SchemaInsightsViewStateProps,
} from "./schemaInsightsView";
import { SchemaInsightEventFilters } from "../types";
import { SortSetting } from "src/sortedtable";
import { actions as localStorageActions } from "../../store/localStorage";
import { Dispatch } from "redux";

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
  },
  onSortChange: (ss: SortSetting) => {
    dispatch(
      localStorageActions.update({
        key: "sortSetting/SchemaInsightsPage",
        value: ss,
      }),
    );
  },
  refreshSchemaInsights: () => {
    dispatch(actions.refresh());
  },
});

export const SchemaInsightsPageConnected = withRouter(
  connect<
    SchemaInsightsViewStateProps,
    SchemaInsightsViewDispatchProps,
    RouteComponentProps
  >(
    mapStateToProps,
    mapDispatchToProps,
  )(SchemaInsightsView),
);
