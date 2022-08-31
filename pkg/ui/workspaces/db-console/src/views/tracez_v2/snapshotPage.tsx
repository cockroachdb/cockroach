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
  SnapshotPage,
  SnapshotPageStateProps,
  SortSetting,
} from "@cockroachlabs/cluster-ui";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { createSelector } from "reselect";
import {
  CachedDataReducerState,
  refreshSnapshot,
  refreshSnapshots,
} from "src/redux/apiReducers";
import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";
import { getMatchParamByName } from "src/util/query";
import { Pick } from "src/util/pick";

export const sortSetting = new LocalSetting<AdminUIState, SortSetting>(
  "sortSetting/spans",
  s => s.localSettings,
  { columnTitle: "creationTime", ascending: false },
);

type SnapshotState = Pick<AdminUIState, "cachedData", "snapshots">;

const selectSnapshotsState = (state: SnapshotState) => {
  if (!state.cachedData.snapshots) {
    return null;
  }

  return state.cachedData.snapshots;
};

const selectSnapshotState = createSelector(
  [
    (state: AdminUIState) => state.cachedData.snapshot,
    (_state: AdminUIState, props: RouteComponentProps) => props,
  ],
  (
    snapshot,
    props,
  ): CachedDataReducerState<api.GetTracingSnapshotResponseMessage> => {
    const snapshotID = getMatchParamByName(props.match, "snapshotID");
    if (!snapshot) {
      return null;
    }
    return snapshot[snapshotID];
  },
);

const mapStateToProps = (
  state: AdminUIState,
  props: RouteComponentProps,
): SnapshotPageStateProps => {
  const sort = sortSetting.selector(state);

  const snapshotsState = selectSnapshotsState(state);
  const snapshots = snapshotsState ? snapshotsState.data : null;
  const snapshotsLoading = snapshotsState ? snapshotsState.inFlight : false;
  const snapshotsError = snapshotsState ? snapshotsState.lastError : null;

  const snapshotState = selectSnapshotState(state, props);
  const snapshot = snapshotState ? snapshotState.data : null;
  const snapshotLoading = snapshotState ? snapshotState.inFlight : false;
  const snapshotError = snapshotState ? snapshotState.lastError : null;

  return {
    sort,

    snapshots,
    snapshotsLoading,
    snapshotsError,

    snapshot,
    snapshotLoading,
    snapshotError,
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
