// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import {
  api,
  ScheduleDetails,
  ScheduleDetailsStateProps,
  selectID,
} from "@cockroachlabs/cluster-ui";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { createSelector } from "reselect";

import { CachedDataReducerState, refreshSchedule } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";

const selectScheduleState = createSelector(
  [(state: AdminUIState) => state.cachedData.schedule, selectID],
  (schedule, scheduleID): CachedDataReducerState<api.Schedule> => {
    if (!schedule) {
      return null;
    }
    return schedule[scheduleID];
  },
);

const mapStateToProps = (
  state: AdminUIState,
  props: RouteComponentProps,
): ScheduleDetailsStateProps => {
  const scheduleState = selectScheduleState(state, props);
  const schedule = scheduleState ? scheduleState.data : null;
  const scheduleLoading = scheduleState ? scheduleState.inFlight : false;
  const scheduleError = scheduleState ? scheduleState.lastError : null;
  return {
    schedule,
    scheduleLoading,
    scheduleError,
  };
};

const mapDispatchToProps = {
  refreshSchedule,
};

export default withRouter(
  connect(mapStateToProps, mapDispatchToProps)(ScheduleDetails),
);
