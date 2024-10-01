// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import {
  JobsPage,
  JobsPageStateProps,
  SortSetting,
  defaultLocalOptions,
} from "@cockroachlabs/cluster-ui";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";

import {
  createSelectorForKeyedCachedDataField,
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

const selectJobsState =
  createSelectorForKeyedCachedDataField<JobsResponseMessage>(
    "jobs",
    (state: AdminUIState, _props) => {
      const type = typeSetting.selector(state);
      const status = statusSetting.selector(state);
      const show = showSetting.selector(state);
      const showAsNum = parseInt(show, 10);
      return jobsKey(status, type, isNaN(showAsNum) ? 0 : showAsNum);
    },
  );

const mapStateToProps = (
  state: AdminUIState,
  props: RouteComponentProps,
): JobsPageStateProps => {
  const sort = sortSetting.selector(state);
  const status = statusSetting.selector(state);
  const show = showSetting.selector(state);
  const type = typeSetting.selector(state);
  const columns = columnsLocalSetting.selectorToArray(state);
  const jobsResponse = selectJobsState(state, props);
  return {
    sort,
    status,
    show,
    type,
    columns,
    jobsResponse,
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
