// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import {
  TransactionInsightDetails,
  TransactionInsightDetailsStateProps,
  TransactionInsightDetailsDispatchProps,
} from "@cockroachlabs/cluster-ui";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";

import {
  refreshTxnInsightDetails,
  refreshUserSQLRoles,
} from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { setGlobalTimeScaleAction } from "src/redux/statements";
import { selectTimeScale } from "src/redux/timeScale";
import { selectHasAdminRole } from "src/redux/user";
import {
  selectTxnInsightDetails,
  selectTransactionInsightDetailsError,
  selectTransactionInsightDetailsMaxSizeReached,
} from "src/views/insights/insightsSelectors";

const mapStateToProps = (
  state: AdminUIState,
  props: RouteComponentProps,
): TransactionInsightDetailsStateProps => {
  return {
    insightDetails: selectTxnInsightDetails(state, props),
    insightError: selectTransactionInsightDetailsError(state, props),
    timeScale: selectTimeScale(state),
    hasAdminRole: selectHasAdminRole(state),
    maxSizeApiReached: selectTransactionInsightDetailsMaxSizeReached(
      state,
      props,
    ),
  };
};

const mapDispatchToProps: TransactionInsightDetailsDispatchProps = {
  refreshTransactionInsightDetails: refreshTxnInsightDetails,
  setTimeScale: setGlobalTimeScaleAction,
  refreshUserSQLRoles: refreshUserSQLRoles,
};

const TransactionInsightDetailsPage = withRouter(
  connect<
    TransactionInsightDetailsStateProps,
    TransactionInsightDetailsDispatchProps,
    RouteComponentProps,
    AdminUIState
  >(
    mapStateToProps,
    mapDispatchToProps,
  )(TransactionInsightDetails),
);

export default TransactionInsightDetailsPage;
