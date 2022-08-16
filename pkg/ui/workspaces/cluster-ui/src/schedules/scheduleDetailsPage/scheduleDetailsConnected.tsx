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
import { selectScheduleState } from "../../store/scheduleDetails/schedule.selectors";
import {
  ScheduleDetailsStateProps,
  ScheduleDetailsDispatchProps,
  ScheduleDetails,
} from "./scheduleDetails";
import { ScheduleRequest } from "src/api/schedulesApi";
import { actions as scheduleActions } from "src/store/scheduleDetails";

const mapStateToProps = (state: AppState): ScheduleDetailsStateProps => {
  const scheduleState = selectScheduleState(state);
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
  refreshSchedule: (req: ScheduleRequest) => scheduleActions.refresh(req),
};

export const ScheduleDetailsPageConnected = withRouter(
  connect<
    ScheduleDetailsStateProps,
    ScheduleDetailsDispatchProps,
    RouteComponentProps
  >(
    mapStateToProps,
    mapDispatchToProps,
  )(ScheduleDetails),
);
