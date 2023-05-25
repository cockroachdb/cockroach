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
import { JobRequest } from "src/api/jobsApi";
import { actions as jobActions } from "src/store/jobDetails";
import { selectID } from "../../selectors";

const mapStateToProps = (
  state: AppState,
  props: RouteComponentProps,
): JobDetailsStateProps => {
  const jobID = selectID(state, props);
  return {
    jobRequest: state.adminUI?.job?.cachedData[jobID],
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
