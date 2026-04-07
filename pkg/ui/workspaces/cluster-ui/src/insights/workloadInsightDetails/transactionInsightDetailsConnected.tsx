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

import { TransactionInsightDetails } from "./transactionInsightDetails";

const TransactionInsightDetailsPage: React.FC<RouteComponentProps> = props => {
  const dispatch = useDispatch();
  const timeScale = useSelector(selectTimeScale);

  return (
    <TransactionInsightDetails
      {...props}
      timeScale={timeScale}
      setTimeScale={(ts: TimeScale) => {
        dispatch(localStorageActions.updateTimeScale({ value: ts }));
        dispatch(
          analyticsActions.track({
            name: "TimeScale changed",
            page: "Transaction Insight Details",
            value: ts.key,
          }),
        );
      }}
    />
  );
};

export const TransactionInsightDetailsConnected = withRouter(
  TransactionInsightDetailsPage,
);
