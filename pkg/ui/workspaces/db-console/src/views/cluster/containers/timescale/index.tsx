// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { connect } from "react-redux";
import { AdminUIState } from "src/redux/state";
import * as timewindow from "src/redux/timewindow";
import { TimeScaleDropdown } from "@cockroachlabs/cluster-ui";
import { createSelector } from "reselect";

const scaleSelector = createSelector(
  (state: AdminUIState) => state?.timewindow,
  tw => tw?.scale,
);

const currentWindowSelector = createSelector(
  (state: AdminUIState) => state?.timewindow,
  tw => tw?.currentWindow,
);

export default connect(
  (state: AdminUIState) => ({
    currentScale: scaleSelector(state),
    currentWindow: currentWindowSelector(state),
  }),
  {
    setTimeScale: timewindow.setTimeScale,
    setTimeRange: timewindow.setTimeRange,
  },
)(TimeScaleDropdown);
