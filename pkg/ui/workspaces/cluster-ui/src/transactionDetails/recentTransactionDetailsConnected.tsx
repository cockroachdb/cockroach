// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { RouteComponentProps, withRouter } from "react-router-dom";
import { connect } from "react-redux";
import { Dispatch } from "redux";
import { actions as sessionsActions } from "src/store/sessions";
import { AppState } from "../store";
import {
  RecentTransactionDetails,
  RecentTransactionDetailsDispatchProps,
} from "./recentTransactionDetails";
import { RecentTransactionDetailsStateProps } from ".";
import {
  selectRecentTransaction,
  selectContentionDetailsForTransaction,
} from "src/selectors/recentExecutions.selectors";

// For tenant cases, we don't show information about node, regions and
// diagnostics.
const mapStateToProps = (
  state: AppState,
  props: RouteComponentProps,
): RecentTransactionDetailsStateProps => {
  return {
    transaction: selectRecentTransaction(state, props),
    contentionDetails: selectContentionDetailsForTransaction(state, props),
    match: props.match,
  };
};

const mapDispatchToProps = (
  dispatch: Dispatch,
): RecentTransactionDetailsDispatchProps => ({
  refreshLiveWorkload: () => dispatch(sessionsActions.refresh()),
});

export const RecentTransactionDetailsPageConnected = withRouter(
  connect(mapStateToProps, mapDispatchToProps)(RecentTransactionDetails),
);
