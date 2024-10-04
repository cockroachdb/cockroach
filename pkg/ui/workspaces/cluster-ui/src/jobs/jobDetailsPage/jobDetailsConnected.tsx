// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";

import { AppState } from "src/store";
import {
  selectJob,
  selectJobLoading,
  selectJobError,
} from "../../store/jobDetails/job.selectors";
import {
  JobDetailsStateProps,
  JobDetailsDispatchProps,
  JobDetails,
} from "./jobDetails";
import { JobRequest } from "src/api/jobsApi";
import { actions as jobActions } from "src/store/jobDetails";

const mapStateToProps = (
  state: AppState,
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
  refreshJob: (req: JobRequest) => jobActions.refresh(req),
};

export const JobDetailsPageConnected = withRouter(
  connect<JobDetailsStateProps, JobDetailsDispatchProps, RouteComponentProps>(
    mapStateToProps,
    mapDispatchToProps,
  )(JobDetails),
);
