// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  SnapshotPage,
  SnapshotPageStateProps,
  SortSetting,
  api as clusterUiApi,
} from "@cockroachlabs/cluster-ui";
import Long from "long";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";

import {
  rawTraceKey,
  refreshNodes,
  refreshRawTrace,
  refreshSnapshot,
  refreshSnapshots,
  snapshotKey,
} from "src/redux/apiReducers";
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
  const nodesState = state.cachedData.nodes;
  const nodeID = getMatchParamByName(props.match, "nodeID");

  const snapshotsState = state.cachedData.snapshots[nodeID];

  const snapshotID = parseInt(getMatchParamByName(props.match, "snapshotID"));
  const snapshotState =
    state.cachedData.snapshot[
      snapshotKey({
        nodeID,
        snapshotID,
      })
    ];

  const spanID = getMatchParamByName(props.match, "spanID");
  let traceID: Long | null = null;
  if (spanID) {
    const span = snapshotState?.data?.snapshot.spans.find(s =>
      s.span_id.equals(spanID),
    );
    traceID = span?.trace_id;
  }
  const rawTraceState =
    state.cachedData.rawTrace[rawTraceKey({ nodeID, snapshotID, traceID })];

  return {
    sort: sortSetting.selector(state),

    nodes: nodesState ? nodesState.data : null,

    snapshots: snapshotsState ? snapshotsState.data : null,
    snapshotsLoading: snapshotsState ? snapshotsState.inFlight : false,
    snapshotsError: snapshotsState ? snapshotsState.lastError : null,

    snapshot: snapshotState ? snapshotState.data : null,
    snapshotLoading: snapshotState ? snapshotState.inFlight : false,
    snapshotError: snapshotState ? snapshotState.lastError : null,

    rawTrace: rawTraceState ? rawTraceState.data : null,
    rawTraceLoading: rawTraceState ? rawTraceState.inFlight : false,
    rawTraceError: rawTraceState ? rawTraceState.lastError : null,

    takeSnapshot: clusterUiApi.takeTracingSnapshot,
    setTraceRecordingType: clusterUiApi.setTraceRecordingType,
  };
};

const mapDispatchToProps = {
  setSort: sortSetting.set,
  refreshNodes,
  refreshSnapshots,
  refreshSnapshot,
  refreshRawTrace,
};

export default withRouter(
  connect(mapStateToProps, mapDispatchToProps)(SnapshotPage),
);
