// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  ActiveTransactionDetails,
  ActiveTransactionDetailsStateProps,
  ActiveTransactionDetailsDispatchProps,
} from "@cockroachlabs/cluster-ui";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";

import { refreshLiveWorkload } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import {
  selectActiveTransaction,
  selectContentionDetailsForTransaction,
} from "src/selectors";

const ActiveTransactionDetailsConnected = withRouter(
  connect<
    ActiveTransactionDetailsStateProps,
    ActiveTransactionDetailsDispatchProps,
    RouteComponentProps,
    AdminUIState
  >(
    (state: AdminUIState, props: RouteComponentProps) => ({
      transaction: selectActiveTransaction(state, props),
      contentionDetails: selectContentionDetailsForTransaction(state, props),
      match: props.match,
    }),
    { refreshLiveWorkload },
  )(ActiveTransactionDetails),
);

export default ActiveTransactionDetailsConnected;
