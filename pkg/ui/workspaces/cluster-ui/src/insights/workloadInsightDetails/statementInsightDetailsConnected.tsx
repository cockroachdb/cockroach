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
import { AppState } from "src/store";
import {
  actions as statementInsights,
  selectStatementInsightDetails,
  selectStatementInsightsError,
} from "src/store/insights/statementInsights";
import { selectIsTenant } from "src/store/uiConfig";
import { TimeScale } from "../../timeScaleDropdown";
import { actions as sqlStatsActions } from "../../store/sqlStats";

const mapStateToProps = (
  state: AppState,
  props: RouteComponentProps,
): StatementInsightDetailsStateProps => {
  const insightStatements = selectStatementInsightDetails(state, props);
  const insightError = selectStatementInsightsError(state);
  return {
    insightEventDetails: insightStatements,
    insightError: insightError,
    isTenant: selectIsTenant(state),
  };
};

const mapDispatchToProps = (
  dispatch: Dispatch,
): StatementInsightDetailsDispatchProps => ({
  refreshStatementInsights: () => {
    dispatch(statementInsights.refresh());
  },
  setTimeScale: (ts: TimeScale) => {
    dispatch(
      sqlStatsActions.updateTimeScale({
        ts: ts,
      }),
    );
  },
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
