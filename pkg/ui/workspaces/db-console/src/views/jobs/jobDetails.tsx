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
} from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";

const selectJob = createSelectorForKeyedCachedDataField("job", selectID);

const mapStateToProps = (
  state: AdminUIState,
  props: RouteComponentProps,
): JobDetailsStateProps => {
  return {
    jobRequest: selectJob(state, props),
  };
};

const mapDispatchToProps = {
  refreshJob,
};

export default withRouter(
  connect(mapStateToProps, mapDispatchToProps)(JobDetails),
);
