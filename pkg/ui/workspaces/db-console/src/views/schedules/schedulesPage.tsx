// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import {
  SchedulesPage,
  SchedulesPageStateProps,
  SortSetting,
} from "@cockroachlabs/cluster-ui";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";

import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";

export const sortSetting = new LocalSetting<AdminUIState, SortSetting>(
  "sortSetting/Schedules",
  s => s.localSettings,
  { columnTitle: "creationTime", ascending: false },
);

const mapStateToProps = (
  state: AdminUIState,
  _: RouteComponentProps,
): SchedulesPageStateProps => {
  const sort = sortSetting.selector(state);
  return {
    sort,
  };
};

const mapDispatchToProps = {
  setSort: sortSetting.set,
};

export default withRouter(
  connect(mapStateToProps, mapDispatchToProps)(SchedulesPage),
);
