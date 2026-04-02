// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { TransactionInsightDetails } from "@cockroachlabs/cluster-ui";
import React from "react";
import { useDispatch, useSelector } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";

import { setGlobalTimeScaleAction } from "src/redux/statements";
import { selectTimeScale } from "src/redux/timeScale";

const TransactionInsightDetailsPageInner: React.FC<
  RouteComponentProps
> = props => {
  const dispatch = useDispatch();
  const timeScale = useSelector(selectTimeScale);

  return (
    <TransactionInsightDetails
      {...props}
      timeScale={timeScale}
      setTimeScale={ts => dispatch(setGlobalTimeScaleAction(ts))}
    />
  );
};

const TransactionInsightDetailsPage = withRouter(
  TransactionInsightDetailsPageInner,
);

export default TransactionInsightDetailsPage;
