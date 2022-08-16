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
  api,
  ScheduleDetails,
  ScheduleDetailsStateProps,
} from "@cockroachlabs/cluster-ui";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { createSelector } from "reselect";
import { CachedDataReducerState, refreshSchedule } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { getMatchParamByName } from "src/util/query";

const selectScheduleState = createSelector(
  [
    (state: AdminUIState) => state.cachedData.schedule,
    (_state: AdminUIState, props: RouteComponentProps) => props,
  ],
  (schedule, props): CachedDataReducerState<api.Schedule> => {
    const scheduleId = getMatchParamByName(props.match, "id");
    if (!schedule) {
      return null;
    }
    return schedule[scheduleId];
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
