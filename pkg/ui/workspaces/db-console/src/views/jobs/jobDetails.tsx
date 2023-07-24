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
  refreshListExecutionDetailFiles,
  refreshJob,
} from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { ListJobProfilerExecutionDetailsResponseMessage } from "src/util/api";
import { api as clusterUiApi } from "@cockroachlabs/cluster-ui";

const selectJob = createSelectorForKeyedCachedDataField("job", selectID);
const selectExecutionDetailFiles =
  createSelectorForKeyedCachedDataField<ListJobProfilerExecutionDetailsResponseMessage>(
    "jobProfiler",
    selectID,
  );
const mapStateToProps = (
  state: AdminUIState,
  props: RouteComponentProps,
): JobDetailsStateProps => {
  const jobID = selectID(state, props);
  return {
    jobRequest: selectJob(state, props),
    jobProfilerExecutionDetailFilesResponse: selectExecutionDetailFiles(
      state,
      props,
    ),
    jobProfilerLastUpdated: state.cachedData.jobProfiler[jobID]?.setAt,
    jobProfilerDataIsValid: state.cachedData.jobProfiler[jobID]?.valid,
    onDownloadExecutionFileClicked: clusterUiApi.getExecutionDetailFile,
  };
};

const mapDispatchToProps = {
  refreshJob,
  refreshExecutionDetailFiles: refreshListExecutionDetailFiles,
};

export default withRouter(
  connect(mapStateToProps, mapDispatchToProps)(JobDetails),
);
