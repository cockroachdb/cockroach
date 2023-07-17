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
  refreshExecutionDetails,
  refreshJob,
} from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { ListJobProfilerExecutionDetailsResponseMessage } from "src/util/api";

const selectJob = createSelectorForKeyedCachedDataField("job", selectID);
const selectExecutionDetails =
  createSelectorForKeyedCachedDataField<ListJobProfilerExecutionDetailsResponseMessage>(
    "jobProfiler",
    selectID,
  );

const mapStateToProps = (
  state: AdminUIState,
  props: RouteComponentProps,
): JobDetailsStateProps => {
  return {
    jobRequest: selectJob(state, props),
    jobProfilerResponse: selectExecutionDetails(state, props),
  };
};

const mapDispatchToProps = {
  refreshJob,
  refreshExecutionDetails,
};

export default withRouter(
  connect(mapStateToProps, mapDispatchToProps)(JobDetails),
);
