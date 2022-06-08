// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import { JobDetails, JobDetailsStateProps } from "@cockroachlabs/cluster-ui";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { createSelector } from "reselect";
import { CachedDataReducerState, refreshJob } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { JobResponseMessage } from "src/util/api";
import { getMatchParamByName } from "src/util/query";

const selectJobState = createSelector(
  [
    (state: AdminUIState) => state.cachedData.job,
    (_state: AdminUIState, props: RouteComponentProps) => props,
  ],
  (job, props): CachedDataReducerState<JobResponseMessage> => {
    const jobId = getMatchParamByName(props.match, "id");
    if (!job) {
      return null;
    }
    return job[jobId];
  },
);

const mapStateToProps = (
  state: AdminUIState,
  props: RouteComponentProps,
): JobDetailsStateProps => {
  const jobState = selectJobState(state, props);
  const job = jobState ? jobState.data : null;
  const jobLoading = jobState ? jobState.inFlight : false;
  const jobError = jobState ? jobState.lastError : null;
  return {
    job,
    jobLoading,
    jobError,
  };
};

const mapDispatchToProps = {
  refreshJob,
};

export default withRouter(
  connect(mapStateToProps, mapDispatchToProps)(JobDetails),
);
