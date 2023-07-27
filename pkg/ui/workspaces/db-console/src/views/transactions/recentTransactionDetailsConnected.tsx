// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
