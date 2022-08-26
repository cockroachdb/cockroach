// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import {
  StatementInsightDetails,
  StatementInsightDetailsStateProps,
} from "@cockroachlabs/cluster-ui";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { AdminUIState } from "src/redux/state";
import { selectStatementInsightDetails } from "src/views/insights/insightsSelectors";

const mapStateToProps = (
  state: AdminUIState,
  props: RouteComponentProps,
): StatementInsightDetailsStateProps => {
  const insightStatements = selectStatementInsightDetails(state, props);
  const insightError = state.cachedData?.insights.lastError;
  return {
    insightEventDetails: insightStatements,
    insightError: insightError,
  };
};

const StatementInsightDetailsPageConnected = withRouter(
  connect<StatementInsightDetailsStateProps, RouteComponentProps>(
    mapStateToProps,
  )(StatementInsightDetails),
);

export default StatementInsightDetailsPageConnected;
