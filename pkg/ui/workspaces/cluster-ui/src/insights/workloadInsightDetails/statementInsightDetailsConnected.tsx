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
import { Dispatch } from "redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import {
  StatementInsightDetails,
  StatementInsightDetailsDispatchProps,
  StatementInsightDetailsStateProps,
} from "./statementInsightDetails";
import { AppState, uiConfigActions } from "src/store";
import {
  actions as statementInsights,
  selectStatementInsightDetails,
  selectExecutionInsightsError,
} from "src/store/insights/statementInsights";
import { selectHasAdminRole, selectIsTenant } from "src/store/uiConfig";
import { TimeScale } from "../../timeScaleDropdown";
import { actions as sqlStatsActions } from "../../store/sqlStats";
import { selectTimeScale } from "../../store/utils/selectors";
import { ExecutionInsightsRequest } from "../../api";

const mapStateToProps = (
  state: AppState,
  props: RouteComponentProps,
): StatementInsightDetailsStateProps => {
  const insightStatements = selectStatementInsightDetails(state, props);
  const insightError = selectExecutionInsightsError(state);
  return {
    insightEventDetails: insightStatements,
    insightError: insightError,
    isTenant: selectIsTenant(state),
    timeScale: selectTimeScale(state),
    hasAdminRole: selectHasAdminRole(state),
  };
};

const mapDispatchToProps = (
  dispatch: Dispatch,
): StatementInsightDetailsDispatchProps => ({
  refreshStatementInsights: (req: ExecutionInsightsRequest) => {
    dispatch(statementInsights.refresh(req));
  },
  setTimeScale: (ts: TimeScale) => {
    dispatch(
      sqlStatsActions.updateTimeScale({
        ts: ts,
      }),
    );
  },
  refreshUserSQLRoles: () => dispatch(uiConfigActions.refreshUserSQLRoles()),
});

export const StatementInsightDetailsConnected = withRouter(
  connect<
    StatementInsightDetailsStateProps,
    StatementInsightDetailsDispatchProps,
    RouteComponentProps
  >(
    mapStateToProps,
    mapDispatchToProps,
  )(StatementInsightDetails),
);
