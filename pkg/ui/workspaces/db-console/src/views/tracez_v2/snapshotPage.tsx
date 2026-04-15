// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  SnapshotPage,
  SnapshotPageStateProps,
  SortSetting,
  api as clusterUiApi,
  useNodes,
  useTracingSnapshots,
  useTracingSnapshot,
  useRawTrace,
} from "@cockroachlabs/cluster-ui";
import Long from "long";
import React, { useMemo } from "react";
import { useDispatch, useSelector } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";

import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState, AppDispatch } from "src/redux/state";
import { getMatchParamByName } from "src/util/query";

export const sortSetting = new LocalSetting<AdminUIState, SortSetting>(
  "sortSetting/spans",
  s => s.localSettings,
  { columnTitle: "creationTime", ascending: false },
);

function SnapshotPageConnector(props: RouteComponentProps) {
  const dispatch: AppDispatch = useDispatch();
  const { match } = props;

  const nodeID = getMatchParamByName(match, "nodeID");
  const snapshotID = parseInt(getMatchParamByName(match, "snapshotID"));
  const spanID = getMatchParamByName(match, "spanID");

  const sort = useSelector((state: AdminUIState) =>
    sortSetting.selector(state),
  );

  const { nodeStatuses } = useNodes();

  const {
    data: snapshotsData,
    isLoading: snapshotsLoading,
    error: snapshotsError,
    refresh: refreshSnapshots,
  } = useTracingSnapshots(nodeID);

  const {
    data: snapshotData,
    isLoading: snapshotLoading,
    error: snapshotError,
    refresh: refreshSnapshot,
  } = useTracingSnapshot(nodeID, isNaN(snapshotID) ? null : snapshotID);

  const traceID = useMemo(() => {
    if (!spanID) return null;
    const span = snapshotData?.snapshot.spans.find(s =>
      s.span_id.equals(spanID),
    );
    return span?.trace_id ?? null;
  }, [spanID, snapshotData]);

  const {
    data: rawTraceData,
    isLoading: rawTraceLoading,
    error: rawTraceError,
    refresh: refreshRawTrace,
  } = useRawTrace(
    nodeID,
    isNaN(snapshotID) ? null : snapshotID,
    traceID,
  );

  const stateProps: SnapshotPageStateProps = {
    sort,
    nodes: nodeStatuses ?? null,
    snapshots: snapshotsData ?? null,
    snapshotsLoading,
    snapshotsError: snapshotsError ?? null,
    snapshot: snapshotData ?? null,
    snapshotLoading,
    snapshotError: snapshotError ?? null,
    rawTrace: rawTraceData ?? null,
    rawTraceLoading,
    rawTraceError: rawTraceError ?? null,
    takeSnapshot: clusterUiApi.takeTracingSnapshot,
    setTraceRecordingType: clusterUiApi.setTraceRecordingType,
  };

  const dispatchProps = useMemo(
    () => ({
      setSort: (value: SortSetting) => dispatch(sortSetting.set(value)),
      refreshNodes: () => {
        /* SWR handles refetching automatically */
      },
      refreshSnapshots: () => refreshSnapshots(),
      refreshSnapshot: () => refreshSnapshot(),
      refreshRawTrace: () => refreshRawTrace(),
    }),
    [dispatch, refreshSnapshots, refreshSnapshot, refreshRawTrace],
  );

  return <SnapshotPage {...props} {...stateProps} {...dispatchProps} />;
}

export default withRouter(SnapshotPageConnector);
