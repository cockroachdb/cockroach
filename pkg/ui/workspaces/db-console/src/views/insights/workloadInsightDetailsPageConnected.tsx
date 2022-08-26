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
  api,
  InsightDetails,
  InsightDetailsDispatchProps,
  InsightDetailsStateProps,
} from "@cockroachlabs/cluster-ui";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { refreshInsightDetails } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { selectInsightDetails } from "src/views/insights/insightsSelectors";

const mapStateToProps = (
  state: AdminUIState,
  props: RouteComponentProps,
): InsightDetailsStateProps => {
  const insightDetailsState = selectInsightDetails(state, props);
  const insight: api.InsightEventDetailsResponse = insightDetailsState?.data;
  const insightError = insightDetailsState?.lastError;
  return {
    insightEventDetails: insight,
    insightError: insightError,
  };
};

const mapDispatchToProps = {
  refreshInsightDetails: refreshInsightDetails,
};

const WorkloadInsightDetailsPageConnected = withRouter(
  connect<
    InsightDetailsStateProps,
    InsightDetailsDispatchProps,
    RouteComponentProps
  >(
    mapStateToProps,
    mapDispatchToProps,
  )(InsightDetails),
);

export default WorkloadInsightDetailsPageConnected;
