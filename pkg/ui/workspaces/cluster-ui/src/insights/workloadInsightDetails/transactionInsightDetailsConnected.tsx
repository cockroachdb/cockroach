// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import React from "react";
import { useDispatch, useSelector } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";

import { AppState, uiConfigActions } from "src/store";

import { actions as analyticsActions } from "../../store/analytics";
import { actions as sqlStatsActions } from "../../store/sqlStats";
import { selectHasAdminRole } from "../../store/uiConfig";
import { selectTimeScale } from "../../store/utils/selectors";
import { TimeScale } from "../../timeScaleDropdown";

import { TransactionInsightDetails } from "./transactionInsightDetails";

const TransactionInsightDetailsPage: React.FC<RouteComponentProps> = props => {
  const dispatch = useDispatch();
  const timeScale = useSelector(selectTimeScale);
  const hasAdminRole = useSelector((state: AppState) =>
    selectHasAdminRole(state),
  );

  return (
    <TransactionInsightDetails
      {...props}
      timeScale={timeScale}
      hasAdminRole={hasAdminRole}
      setTimeScale={(ts: TimeScale) => {
        dispatch(sqlStatsActions.updateTimeScale({ ts }));
        dispatch(
          analyticsActions.track({
            name: "TimeScale changed",
            page: "Transaction Insight Details",
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

export const TransactionInsightDetailsConnected = withRouter(
  TransactionInsightDetailsPage,
);
