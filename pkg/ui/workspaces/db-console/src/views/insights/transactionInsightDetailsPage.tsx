// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { TransactionInsightDetails } from "@cockroachlabs/cluster-ui";
import React from "react";
import { useDispatch, useSelector } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";

import { refreshUserSQLRoles } from "src/redux/apiReducers";
import { setGlobalTimeScaleAction } from "src/redux/statements";
import { selectTimeScale } from "src/redux/timeScale";
import { selectHasAdminRole } from "src/redux/user";

const TransactionInsightDetailsPageInner: React.FC<
  RouteComponentProps
> = props => {
  const dispatch = useDispatch();
  const timeScale = useSelector(selectTimeScale);
  const hasAdminRole = useSelector(selectHasAdminRole);

  return (
    <TransactionInsightDetails
      {...props}
      timeScale={timeScale}
      hasAdminRole={hasAdminRole}
      setTimeScale={ts => dispatch(setGlobalTimeScaleAction(ts))}
      refreshUserSQLRoles={() => dispatch(refreshUserSQLRoles())}
    />
  );
};

const TransactionInsightDetailsPage = withRouter(
  TransactionInsightDetailsPageInner,
);

export default TransactionInsightDetailsPage;
