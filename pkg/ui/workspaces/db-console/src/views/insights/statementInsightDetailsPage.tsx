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
  StatementInsightDetailsDispatchProps,
} from "@cockroachlabs/cluster-ui";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { AdminUIState } from "src/redux/state";
import {
  refreshStatementInsights,
  refreshUserSQLRoles,
} from "src/redux/apiReducers";
import { selectStatementInsightDetails } from "src/views/insights/insightsSelectors";
import { setGlobalTimeScaleAction } from "src/redux/statements";
import { selectHasAdminRole } from "src/redux/user";

const mapStateToProps = (
  state: AdminUIState,
  props: RouteComponentProps,
): StatementInsightDetailsStateProps => {
  const insightStatements = selectStatementInsightDetails(state, props);
  const insightError = state.cachedData?.statementInsights.lastError;
  return {
    insightEventDetails: insightStatements,
    insightError: insightError,
    hasAdminRole: selectHasAdminRole(state),
  };
};

const mapDispatchToProps: StatementInsightDetailsDispatchProps = {
  setTimeScale: setGlobalTimeScaleAction,
  refreshStatementInsights: refreshStatementInsights,
  refreshUserSQLRoles: refreshUserSQLRoles,
};

const StatementInsightDetailsPage = withRouter(
  connect<
    StatementInsightDetailsStateProps,
    StatementInsightDetailsDispatchProps,
    RouteComponentProps
  >(
    mapStateToProps,
    mapDispatchToProps,
  )(StatementInsightDetails),
);

export default StatementInsightDetailsPage;
