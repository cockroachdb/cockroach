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
  JobDetails,
  JobDetailsStateProps,
  selectID,
} from "@cockroachlabs/cluster-ui";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { createSelector } from "reselect";
import { KeyedCachedDataReducerState, refreshJob } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { JobResponseMessage } from "src/util/api";

const selectJobState = createSelector(
  [(state: AdminUIState) => state.cachedData?.job],
  (jobState): KeyedCachedDataReducerState<JobResponseMessage> => {
    if (!jobState) {
      return null;
    }
    return jobState;
  },
);

export const selectJob = createSelector(
  [(state: AdminUIState) => state.cachedData?.jobs, selectJobState, selectID],
  (jobsState, jobState, jobID) => {
    const jobsCache = jobsState.data;
    let job: JobResponseMessage;
    if (!jobID || (!jobsCache && !jobState)) {
      return null;
    } else if (jobsCache) {
      job = Object(
        jobsCache.data.jobs.find(job => job.id.toString() === jobID),
      );
    } else if (jobState) {
      job = jobState[jobID]?.data;
    }
    return job;
  },
);

export const selectJobError = createSelector(
  selectJobState,
  selectID,
  (state: KeyedCachedDataReducerState<JobResponseMessage>, jobID) => {
    if (!state || !jobID) {
      return null;
    }
    return state[jobID]?.lastError;
  },
);

export const selectJobLoading = createSelector(
  selectJobState,
  selectID,
  (state: KeyedCachedDataReducerState<JobResponseMessage>, jobID) => {
    if (!state || !jobID) {
      return null;
    }
    return state[jobID]?.inFlight;
  },
);

const mapStateToProps = (
  state: AdminUIState,
  props: RouteComponentProps,
): JobDetailsStateProps => {
  const job = selectJob(state, props);
  const jobLoading = selectJobLoading(state, props);
  const jobError = selectJobError(state, props);
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
