// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import React from "react";
import { useDispatch, useSelector } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";

import { actions as analyticsActions } from "../../store/analytics";
import { actions as localStorageActions } from "../../store/localStorage";
import { selectTimeScale } from "../../store/utils/selectors";
import { TimeScale } from "../../timeScaleDropdown";

import { StatementInsightDetails } from "./statementInsightDetails";

const StatementInsightDetailsPage: React.FC<RouteComponentProps> = props => {
  const dispatch = useDispatch();
  const timeScale = useSelector(selectTimeScale);

  return (
    <StatementInsightDetails
      {...props}
      timeScale={timeScale}
      setTimeScale={(ts: TimeScale) => {
        dispatch(localStorageActions.updateTimeScale({ value: ts }));
        dispatch(
          analyticsActions.track({
            name: "TimeScale changed",
            page: "Statement Insight Details",
            value: ts.key,
          }),
        );
      }}
    />
  );
};

export const StatementInsightDetailsConnected = withRouter(
  StatementInsightDetailsPage,
);
