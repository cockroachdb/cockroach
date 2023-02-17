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
  JobsPage,
  JobsPageStateProps,
  SortSetting,
  defaultLocalOptions,
} from "@cockroachlabs/cluster-ui";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { createSelector } from "reselect";
import {
  CachedDataReducerState,
  jobsKey,
  refreshJobs,
} from "src/redux/apiReducers";
import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";
import { JobsResponseMessage } from "src/util/api";

export const statusSetting = new LocalSetting<AdminUIState, string>(
  "jobs/status_setting",
  s => s.localSettings,
  defaultLocalOptions.status,
);

export const typeSetting = new LocalSetting<AdminUIState, number>(
  "jobs/type_setting",
  s => s.localSettings,
  defaultLocalOptions.type,
);

export const showSetting = new LocalSetting<AdminUIState, string>(
  "jobs/show_setting",
  s => s.localSettings,
  defaultLocalOptions.show,
);

export const sortSetting = new LocalSetting<AdminUIState, SortSetting>(
  "sortSetting/Jobs",
  s => s.localSettings,
  { columnTitle: "creationTime", ascending: false },
);

export const columnsLocalSetting = new LocalSetting<AdminUIState, string[]>(
  "jobs/column_setting",
  s => s.localSettings,
  null,
);

const selectJobsState = createSelector(
  [
    (state: AdminUIState) => state.cachedData.jobs,
    (_state: AdminUIState, key: string) => key,
  ],
  (jobs, key): CachedDataReducerState<JobsResponseMessage> => {
    if (!jobs) {
      return null;
    }
    return jobs[key];
  },
);

const mapStateToProps = (
  state: AdminUIState,
  _: RouteComponentProps,
): JobsPageStateProps => {
  const sort = sortSetting.selector(state);
  const status = statusSetting.selector(state);
  const show = showSetting.selector(state);
  const type = typeSetting.selector(state);
  const columns = columnsLocalSetting.selectorToArray(state);
  const showAsNum = parseInt(show, 10);
  const key = jobsKey(status, type, isNaN(showAsNum) ? 0 : showAsNum);
  const jobsState = selectJobsState(state, key);
  const jobs = jobsState ? jobsState.data : null;
  const jobsError = jobsState ? jobsState.lastError : null;
  const lastUpdated = jobsError ? jobsState.requestedAt : jobsState?.setAt;
  return {
    sort,
    status,
    show,
    type,
    jobs,
    reqInFlight: jobsState?.inFlight,
    isDataValid: jobsState?.valid,
    jobsError,
    columns,
    lastUpdated,
  };
};

const mapDispatchToProps = {
  setSort: sortSetting.set,
  setStatus: statusSetting.set,
  setShow: showSetting.set,
  setType: typeSetting.set,
  onColumnsChange: columnsLocalSetting.set,
  refreshJobs,
};

export default withRouter(
  connect(mapStateToProps, mapDispatchToProps)(JobsPage),
);
