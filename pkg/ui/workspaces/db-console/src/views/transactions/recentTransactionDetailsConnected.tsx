// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  RecentTransactionDetails,
  RecentTransactionDetailsStateProps,
  RecentTransactionDetailsDispatchProps,
} from "@cockroachlabs/cluster-ui";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { AdminUIState } from "src/redux/state";
import { refreshLiveWorkload } from "src/redux/apiReducers";
import {
  selectRecentTransaction,
  selectContentionDetailsForTransaction,
} from "src/selectors";

const RecentTransactionDetailsConnected = withRouter(
  connect<
    RecentTransactionDetailsStateProps,
    RecentTransactionDetailsDispatchProps,
    RouteComponentProps
  >(
    (state: AdminUIState, props: RouteComponentProps) => ({
      transaction: selectRecentTransaction(state, props),
      contentionDetails: selectContentionDetailsForTransaction(state, props),
      match: props.match,
    }),
    { refreshLiveWorkload },
  )(RecentTransactionDetails),
);

export default RecentTransactionDetailsConnected;
