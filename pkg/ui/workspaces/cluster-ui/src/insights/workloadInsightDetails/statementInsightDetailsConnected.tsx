// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { Dispatch } from "redux";

import { AppState, uiConfigActions } from "src/store";
import {
  selectStmtInsightDetails,
  selectStmtInsightsError,
} from "src/store/insights/statementInsights";
import { selectHasAdminRole } from "src/store/uiConfig";

import { actions as analyticsActions } from "../../store/analytics";
import { actions as sqlStatsActions } from "../../store/sqlStats";
import { selectTimeScale } from "../../store/utils/selectors";
import { TimeScale } from "../../timeScaleDropdown";

import {
  StatementInsightDetails,
  StatementInsightDetailsDispatchProps,
  StatementInsightDetailsStateProps,
} from "./statementInsightDetails";

const mapStateToProps = (
  state: AppState,
  props: RouteComponentProps,
): StatementInsightDetailsStateProps => {
  const insightStatements = selectStmtInsightDetails(state, props);
  const insightError = selectStmtInsightsError(state);
  return {
    insightEventDetails: insightStatements,
    insightError: insightError,
    timeScale: selectTimeScale(state),
    hasAdminRole: selectHasAdminRole(state),
  };
};

const mapDispatchToProps = (
  dispatch: Dispatch,
): StatementInsightDetailsDispatchProps => ({
  setTimeScale: (ts: TimeScale) => {
    dispatch(
      sqlStatsActions.updateTimeScale({
        ts: ts,
      }),
    );
    dispatch(
      analyticsActions.track({
        name: "TimeScale changed",
        page: "Statement Insight Details",
        value: ts.key,
      }),
    );
  },
  refreshUserSQLRoles: () => dispatch(uiConfigActions.refreshUserSQLRoles()),
});

export const StatementInsightDetailsConnected = withRouter(
  connect<
    StatementInsightDetailsStateProps,
    StatementInsightDetailsDispatchProps,
    RouteComponentProps,
    AppState
  >(
    mapStateToProps,
    mapDispatchToProps,
  )(StatementInsightDetails),
);
