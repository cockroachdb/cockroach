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
import {
  createSelectorForKeyedCachedDataField,
  refreshJob,
  refreshJobProfilerBundles,
} from "src/redux/apiReducers";
import { AdminUIState, AppDispatch } from "src/redux/state";
import { selectJobProfilerBundlesByJobID } from "src/redux/jobs/jobsSelectors";
import { api as clusterUiApi } from "@cockroachlabs/cluster-ui";
import { createJobProfilerBundleAction } from "src/redux/jobs/jobsActions";

const selectJob = createSelectorForKeyedCachedDataField("job", selectID);

const mapStateToProps = (
  state: AdminUIState,
  props: RouteComponentProps,
): JobDetailsStateProps => {
  const jobID = selectID(state, props);
  return {
    jobRequest: selectJob(state, props),
    bundles: selectJobProfilerBundlesByJobID(state, jobID),
    downloadBundle: clusterUiApi.getJobProfilerBundle,
  };
};

const mapDispatchToProps = {
  refreshJob,
  refreshJobProfilerBundles,
  onCollectJobProfilerBundle: (
    insertJobProfilerBundleRequest: clusterUiApi.InsertJobProfilerBundleRequest,
  ) => {
    return (dispatch: AppDispatch) => {
      dispatch(createJobProfilerBundleAction(insertJobProfilerBundleRequest));
    };
  },
};

export default withRouter(
  connect(mapStateToProps, mapDispatchToProps)(JobDetails),
);
