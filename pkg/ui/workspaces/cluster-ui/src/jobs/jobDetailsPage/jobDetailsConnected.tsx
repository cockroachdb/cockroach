// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";

import { AppState } from "src/store";
import {
  JobDetailsStateProps,
  JobDetailsDispatchProps,
  JobDetails,
} from "./jobDetails";
import { JobRequest, JobResponse } from "src/api/jobsApi";
import { actions as jobActions } from "src/store/jobDetails";
import { selectID } from "../../selectors";
import {
  ListJobProfilerExecutionDetailsRequest,
  createInitialState,
  getExecutionDetailFile,
} from "src/api";
import {
  initialState,
  actions as jobProfilerActions,
} from "src/store/jobs/jobProfiler.reducer";
import { Dispatch } from "redux";

const emptyState = createInitialState<JobResponse>();

const mapStateToProps = (
  state: AppState,
  props: RouteComponentProps,
): JobDetailsStateProps => {
  const jobID = selectID(state, props);
  return {
    jobRequest: state.adminUI?.job?.cachedData[jobID] ?? emptyState,
    jobProfilerExecutionDetailFilesResponse:
      state.adminUI?.executionDetailFiles ?? initialState,
    jobProfilerLastUpdated: state.adminUI?.executionDetailFiles?.lastUpdated,
    jobProfilerDataIsValid: state.adminUI?.executionDetailFiles?.valid,
    onDownloadExecutionFileClicked: getExecutionDetailFile,
  };
};

const mapDispatchToProps = (dispatch: Dispatch): JobDetailsDispatchProps => ({
  refreshJob: (req: JobRequest) => jobActions.refresh(req),
  refreshExecutionDetailFiles: (req: ListJobProfilerExecutionDetailsRequest) =>
    dispatch(jobProfilerActions.refresh(req)),
});

export const JobDetailsPageConnected = withRouter(
  connect<JobDetailsStateProps, JobDetailsDispatchProps, RouteComponentProps>(
    mapStateToProps,
    mapDispatchToProps,
  )(JobDetails),
);
