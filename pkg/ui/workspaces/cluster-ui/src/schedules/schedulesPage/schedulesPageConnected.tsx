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
  selectSchedulesState,
  selectShowSetting,
  selectSortSetting,
  selectStatusSetting,
} from "../../store/schedules/schedules.selectors";
import {
  SchedulesPageStateProps,
  SchedulesPageDispatchProps,
  SchedulesPage,
} from "./schedulesPage";
import { SchedulesRequest } from "src/api/schedulesApi";
import { actions as schedulesActions } from "src/store/schedules";
import { actions as localStorageActions } from "../../store/localStorage";
import { Dispatch } from "redux";
import { SortSetting } from "../../sortedtable";

const mapStateToProps = (
  state: AppState,
  _: RouteComponentProps,
): SchedulesPageStateProps => {
  const sort = selectSortSetting(state);
  const status = selectStatusSetting(state);
  const show = selectShowSetting(state);
  const schedulesState = selectSchedulesState(state);
  const schedules = schedulesState ? schedulesState.data : null;
  const schedulesLoading = schedulesState ? schedulesState.inFlight : false;
  const schedulesError = schedulesState ? schedulesState.lastError : null;
  return {
    sort,
    status,
    show,
    schedules,
    schedulesLoading,
    schedulesError,
  };
};

const mapDispatchToProps = (
  dispatch: Dispatch,
): SchedulesPageDispatchProps => ({
  setShow: (showValue: string) => {
    dispatch(
      localStorageActions.update({
        key: "showSetting/SchedulesPage",
        value: showValue,
      }),
    );
  },
  setSort: (ss: SortSetting) => {
    dispatch(
      localStorageActions.update({
        key: "sortSetting/SchedulesPage",
        value: ss,
      }),
    );
  },
  setStatus: (statusValue: string) => {
    dispatch(
      localStorageActions.update({
        key: "statusSetting/SchedulesPage",
        value: statusValue,
      }),
    );
  },
  refreshSchedules: (req: SchedulesRequest) =>
    dispatch(schedulesActions.refresh(req)),
  onFilterChange: (req: SchedulesRequest) =>
    dispatch(schedulesActions.updateFilteredSchedules(req)),
});

export const SchedulesPageConnected = withRouter(
  connect<
    SchedulesPageStateProps,
    SchedulesPageDispatchProps,
    RouteComponentProps
  >(
    mapStateToProps,
    mapDispatchToProps,
  )(SchedulesPage),
);
