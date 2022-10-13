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
  SnapshotPage,
  SnapshotPageStateProps,
  SortSetting,
} from "@cockroachlabs/cluster-ui";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { refreshSnapshot, refreshSnapshots } from "src/redux/apiReducers";
import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";
import { getMatchParamByName } from "src/util/query";

export const sortSetting = new LocalSetting<AdminUIState, SortSetting>(
  "sortSetting/spans",
  s => s.localSettings,
  { columnTitle: "creationTime", ascending: false },
);

const mapStateToProps = (
  state: AdminUIState,
  props: RouteComponentProps,
): SnapshotPageStateProps => {
  const snapshotsState = state.cachedData.snapshots;

  const snapshotID = getMatchParamByName(props.match, "snapshotID");
  const snapshotState = state.cachedData.snapshot[snapshotID];

  return {
    sort: sortSetting.selector(state),

    snapshots: snapshotsState ? snapshotsState.data : null,
    snapshotsLoading: snapshotsState ? snapshotsState.inFlight : false,
    snapshotsError: snapshotsState ? snapshotsState.lastError : null,

    snapshot: snapshotState ? snapshotState.data : null,
    snapshotLoading: snapshotState ? snapshotState.inFlight : false,
    snapshotError: snapshotState ? snapshotState.lastError : null,
  };
};

const mapDispatchToProps = {
  setSort: sortSetting.set,
  refreshSnapshots,
  refreshSnapshot,
};

export default withRouter(
  connect(mapStateToProps, mapDispatchToProps)(SnapshotPage),
);
