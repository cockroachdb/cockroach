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
  StatementInsightDetails,
  StatementInsightDetailsStateProps,
} from "./statementInsightDetails";
import { AppState } from "src/store";
import {
  selectStatementInsightDetails,
  selectStatementInsightsError,
} from "src/store/insights/statementInsights";

const mapStateToProps = (
  state: AppState,
  props: RouteComponentProps,
): StatementInsightDetailsStateProps => {
  const insightStatements = selectStatementInsightDetails(state, props);
  const insightError = selectStatementInsightsError(state);
  return {
    insightEventDetails: insightStatements,
    insightError: insightError,
  };
};

export const StatementInsightDetailsConnected = withRouter(
  connect<StatementInsightDetailsStateProps, RouteComponentProps>(
    mapStateToProps,
  )(StatementInsightDetails),
);
