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
  TransactionInsightDetails,
  TransactionInsightDetailsStateProps,
  TransactionInsightDetailsDispatchProps,
  api,
} from "@cockroachlabs/cluster-ui";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { refreshTransactionInsightDetails } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { selectTransactionInsightDetails } from "src/views/insights/insightsSelectors";

const mapStateToProps = (
  state: AdminUIState,
  props: RouteComponentProps,
): TransactionInsightDetailsStateProps => {
  const insightDetailsState = selectTransactionInsightDetails(state, props);
  const insight: api.TransactionInsightEventDetailsResponse =
    insightDetailsState?.data;
  const insightError = insightDetailsState?.lastError;
  return {
    insightEventDetails: insight,
    insightError: insightError,
  };
};

const mapDispatchToProps = {
  refreshTransactionInsightDetails: refreshTransactionInsightDetails,
};

const TransactionInsightDetailsPageConnected = withRouter(
  connect<
    TransactionInsightDetailsStateProps,
    TransactionInsightDetailsDispatchProps,
    RouteComponentProps
  >(
    mapStateToProps,
    mapDispatchToProps,
  )(TransactionInsightDetails),
);

export default TransactionInsightDetailsPageConnected;
