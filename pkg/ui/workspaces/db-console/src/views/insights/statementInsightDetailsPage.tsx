// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import {
  StatementInsightDetails,
  StatementInsightDetailsStateProps,
  StatementInsightDetailsDispatchProps,
} from "@cockroachlabs/cluster-ui";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { AdminUIState } from "src/redux/state";
import { refreshUserSQLRoles } from "src/redux/apiReducers";
import { selectStatementInsightDetails } from "src/views/insights/insightsSelectors";
import { setGlobalTimeScaleAction } from "src/redux/statements";
import { selectTimeScale } from "src/redux/timeScale";
import { selectHasAdminRole } from "src/redux/user";

const mapStateToProps = (
  state: AdminUIState,
  props: RouteComponentProps,
): StatementInsightDetailsStateProps => {
  return {
    insightEventDetails: selectStatementInsightDetails(state, props),
    insightError: state.cachedData?.stmtInsights?.lastError,
    timeScale: selectTimeScale(state),
    hasAdminRole: selectHasAdminRole(state),
  };
};

const mapDispatchToProps: StatementInsightDetailsDispatchProps = {
  setTimeScale: setGlobalTimeScaleAction,
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
