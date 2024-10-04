// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";

import { AppState, uiConfigActions } from "src/store";
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
import long from "long";
import { selectHasAdminRole } from "src/store/uiConfig";

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
    hasAdminRole: selectHasAdminRole(state),
  };
};

const mapDispatchToProps = (dispatch: Dispatch): JobDetailsDispatchProps => ({
  refreshJob: (req: JobRequest) => dispatch(jobActions.refresh(req)),
  refreshExecutionDetailFiles: (req: ListJobProfilerExecutionDetailsRequest) =>
    dispatch(jobProfilerActions.refresh(req)),
  onRequestExecutionDetails: (jobID: long) => {
    dispatch(jobProfilerActions.collectExecutionDetails({ job_id: jobID }));
  },
  refreshUserSQLRoles: () => dispatch(uiConfigActions.refreshUserSQLRoles()),
});

export const JobDetailsPageConnected = withRouter(
  connect<JobDetailsStateProps, JobDetailsDispatchProps, RouteComponentProps>(
    mapStateToProps,
    mapDispatchToProps,
  )(JobDetails),
);
