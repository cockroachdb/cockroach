// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import React from "react";
import { useDispatch, useSelector } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";

import { AppState, uiConfigActions } from "src/store";
import { selectHasAdminRole } from "src/store/uiConfig";

import { actions as analyticsActions } from "../../store/analytics";
import { actions as sqlStatsActions } from "../../store/sqlStats";
import { selectTimeScale } from "../../store/utils/selectors";
import { TimeScale } from "../../timeScaleDropdown";

import { StatementInsightDetails } from "./statementInsightDetails";

const StatementInsightDetailsPage: React.FC<RouteComponentProps> = props => {
  const dispatch = useDispatch();
  const timeScale = useSelector(selectTimeScale);
  const hasAdminRole = useSelector((state: AppState) =>
    selectHasAdminRole(state),
  );

  return (
    <StatementInsightDetails
      {...props}
      timeScale={timeScale}
      hasAdminRole={hasAdminRole}
      setTimeScale={(ts: TimeScale) => {
        dispatch(sqlStatsActions.updateTimeScale({ ts }));
        dispatch(
          analyticsActions.track({
            name: "TimeScale changed",
            page: "Statement Insight Details",
            value: ts.key,
          }),
        );
      }}
      refreshUserSQLRoles={() =>
        dispatch(uiConfigActions.refreshUserSQLRoles())
      }
    />
  );
};

export const StatementInsightDetailsConnected = withRouter(
  StatementInsightDetailsPage,
);
