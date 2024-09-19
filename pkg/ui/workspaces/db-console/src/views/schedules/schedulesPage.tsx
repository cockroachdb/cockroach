// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import {
  api,
  SchedulesPage,
  SchedulesPageStateProps,
  SortSetting,
  showOptions,
  statusOptions,
} from "@cockroachlabs/cluster-ui";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { createSelector } from "reselect";

import {
  CachedDataReducerState,
  schedulesKey,
  refreshSchedules,
} from "src/redux/apiReducers";
import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";

export const statusSetting = new LocalSetting<AdminUIState, string>(
  "schedules/status_setting",
  s => s.localSettings,
  statusOptions[0].value,
);

export const showSetting = new LocalSetting<AdminUIState, string>(
  "schedules/show_setting",
  s => s.localSettings,
  showOptions[0].value,
);

export const sortSetting = new LocalSetting<AdminUIState, SortSetting>(
  "sortSetting/Schedules",
  s => s.localSettings,
  { columnTitle: "creationTime", ascending: false },
);

const selectSchedulesState = createSelector(
  [
    (state: AdminUIState) => state.cachedData.schedules,
    (_state: AdminUIState, key: string) => key,
  ],
  (schedules, key): CachedDataReducerState<api.Schedules> => {
    if (!schedules) {
      return null;
    }
    return schedules[key];
  },
);

const mapStateToProps = (
  state: AdminUIState,
  _: RouteComponentProps,
): SchedulesPageStateProps => {
  const sort = sortSetting.selector(state);
  const status = statusSetting.selector(state);
  const show = showSetting.selector(state);
  const key = schedulesKey({ status, limit: parseInt(show, 10) });
  const schedulesState = selectSchedulesState(state, key);
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

const mapDispatchToProps = {
  setSort: sortSetting.set,
  setStatus: statusSetting.set,
  setShow: showSetting.set,
  refreshSchedules,
};

export default withRouter(
  connect(mapStateToProps, mapDispatchToProps)(SchedulesPage),
);
