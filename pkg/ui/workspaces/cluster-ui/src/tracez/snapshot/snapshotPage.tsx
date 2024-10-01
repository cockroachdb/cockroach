// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { join } from "path";

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import Long from "long";
import React, { useCallback, useEffect } from "react";
import { RouteComponentProps } from "react-router-dom";

import {
  ListTracingSnapshotsResponse,
  GetTracingSnapshotResponse,
  TakeTracingSnapshotResponse,
  SetTraceRecordingTypeResponse,
  RecordingMode,
  GetTraceResponse,
} from "src/api/tracezApi";
import { Breadcrumbs } from "src/breadcrumbs";
import { SortSetting } from "src/sortedtable";
import { getMatchParamByName, syncHistory } from "src/util";

import { RawTraceComponent } from "./rawTraceComponent";
import { SnapshotComponent } from "./snapshotComponent";
import { SpanComponent } from "./spanComponent";

// This component does some manual route management and navigation.
// This is because the data model doesn't match the ideal route form.
// The data model is largely monolithic - one downloads a whole snapshot at a
// time. But often, one only wants to view part of the snapshot, as in e.g.
// the span view.
// In order to provide that feature with respectable performance and an easy
// GUI, we toggle between one of several components here based on the URL
// params. To manage that navigation, we need to know the route prefix.
export const ROUTE_PREFIX = "/debug/tracez/";

export interface SnapshotPageStateProps {
  sort: SortSetting;

  nodes?: cockroach.server.status.statuspb.INodeStatus[];

  snapshots?: ListTracingSnapshotsResponse;
  snapshotsLoading: boolean;
  snapshotsError?: Error;

  snapshot: GetTracingSnapshotResponse;
  snapshotLoading: boolean;
  snapshotError?: Error;

  rawTrace: GetTraceResponse;
  rawTraceLoading: boolean;
  rawTraceError?: Error;

  takeSnapshot: (nodeID: string) => Promise<TakeTracingSnapshotResponse>;
  setTraceRecordingType: (
    nodeID: string,
    traceID: Long,
    recordingMode: RecordingMode,
  ) => Promise<SetTraceRecordingTypeResponse>;
}

export interface SnapshotPageDispatchProps {
  setSort: (value: SortSetting) => void;
  refreshSnapshots: (id: string) => void;
  refreshSnapshot: (req: { nodeID: string; snapshotID: number }) => void;
  refreshNodes: () => void;
  refreshRawTrace: (req: {
    nodeID: string;
    snapshotID: number;
    traceID: Long;
  }) => void;
}

type UrlParams = Partial<
  Record<
    "nodeID" | "snapshotID" | "spanID" | "ascending" | "columnTitle",
    string
  >
>;
export type SnapshotPageProps = SnapshotPageStateProps &
  SnapshotPageDispatchProps &
  RouteComponentProps<UrlParams>;

export const SnapshotPage: React.FC<SnapshotPageProps> = props => {
  const {
    history,
    match,

    sort,
    setSort,

    nodes,
    refreshNodes,

    snapshots,
    snapshotsLoading,
    snapshotsError,
    refreshSnapshots,

    snapshot,
    snapshotLoading,
    snapshotError,
    refreshSnapshot,

    rawTrace,
    rawTraceLoading,
    rawTraceError,
    refreshRawTrace,

    takeSnapshot,
    setTraceRecordingType,
  } = props;

  // Sort Settings.
  const ascending = match.params.ascending === "true";
  const columnTitle = match.params.columnTitle || "";

  // Always an integer ID.
  const snapshotID = parseInt(getMatchParamByName(match, "snapshotID"));
  // Always a Long, or undefined.
  const spanStr = getMatchParamByName(match, "spanID");
  const spanID = spanStr ? Long.fromString(spanStr) : null;
  // Usually a string-wrapped integer ID, but also supports alias "local."
  const nodeID = getMatchParamByName(match, "nodeID");

  const isRaw =
    match.path ===
    join(ROUTE_PREFIX, "node/:nodeID/snapshot/:snapshotID/span/:spanID/raw");

  // Load initial data.
  useEffect(() => {
    refreshNodes();
  }, [refreshNodes]);

  useEffect(() => {
    refreshSnapshots(nodeID);
    // Reload the snapshots when transitioning to a new snapshot ID.
    // This isn't always necessary, but doesn't hurt (it's async) and helps
    // when taking a new snapshot.
  }, [nodeID, snapshotID, refreshSnapshots]);

  useEffect(() => {
    if (!snapshotID) {
      return;
    }
    refreshSnapshot({
      nodeID: nodeID,
      snapshotID: snapshotID,
    });
  }, [nodeID, snapshotID, refreshSnapshot]);

  const snapArray = snapshots?.snapshots;
  const snapArrayAsJson = JSON.stringify(snapArray);

  // If no node was provided, navigate explicitly to the local node.
  // If no snapshot was provided, navigate to the most recent.
  useEffect(() => {
    if (snapshotID) {
      return;
    }

    if (!snapArray?.length) {
      // If we have no snapshots, or the record is stale, don't navigate.
      return;
    }

    const lastSnapshotID = snapArray[snapArray.length - 1].snapshot_id;
    history.location.pathname = join(
      history.location.pathname,
      "snapshot",
      lastSnapshotID.toString(),
    );
    history.replace(history.location);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [snapArrayAsJson, snapshotID, history]);

  // Update sort based on URL.
  useEffect(() => {
    if (!columnTitle) {
      return;
    }
    setSort({ columnTitle, ascending });
  }, [setSort, columnTitle, ascending]);

  const onSnapshotSelected = (item: number) => {
    history.location.pathname = join(
      ROUTE_PREFIX,
      "node",
      nodeID,
      "snapshot",
      item.toString(),
    );
    history.push(history.location);
  };

  const onNodeSelected = (item: string) => {
    history.location.pathname = join(ROUTE_PREFIX, "node/", item);
    history.push(history.location);
  };

  const changeSortSetting = (ss: SortSetting): void => {
    setSort(ss);
    syncHistory(
      {
        ascending: ss.ascending.toString(),
        columnTitle: ss.columnTitle,
      },
      history,
    );
  };

  const takeAndLoadSnapshot = () => {
    takeSnapshot(nodeID).then(resp => {
      // Load the new snapshot.
      history.location.pathname = join(
        ROUTE_PREFIX,
        "node",
        nodeID,
        "/snapshot/",
        resp.snapshot.snapshot_id.toString(),
      );
      history.push(history.location);
    });
  };

  const spanDetailsURL = useCallback(
    (spanID: Long): string => {
      return join(
        ROUTE_PREFIX,
        "node",
        nodeID,
        "/snapshot/",
        snapshotID.toString(),
        "span",
        spanID.toString(),
      );
    },
    [nodeID, snapshotID],
  );

  const rawTraceURL = useCallback(
    (spanID: Long): string => {
      return join(
        ROUTE_PREFIX,
        "node",
        nodeID,
        "/snapshot/",
        snapshotID.toString(),
        "span",
        spanID.toString(),
        "raw",
      );
    },
    [nodeID, snapshotID],
  );

  const isLoading = snapshotsLoading || snapshotLoading;
  const error = snapshotsError || snapshotError;
  const spans = snapshot?.snapshot.spans;
  const span = spanID ? spans?.find(s => s.span_id.equals(spanID)) : null;
  const snapProps = {
    sort,
    changeSortSetting,
    nodes,
    nodeID,
    onNodeSelected,
    snapshots,
    snapshotID,
    snapshot,
    onSnapshotSelected,
    isLoading,
    error,
    spanDetailsURL,
    takeAndLoadSnapshot,
  };
  const spanProps = {
    sort,
    changeSortSetting,

    nodeID,

    snapshot,
    snapshotLoading,
    snapshotError,

    span,
    spanDetailsURL,
    setTraceRecordingType,

    rawTraceURL,
  };
  const rawTraceProps = {
    nodeID,
    snapshotID,
    traceID: span?.trace_id,

    rawTrace,
    rawTraceLoading,
    rawTraceError,
    refreshRawTrace,
  };

  const breadcrumbItems = [
    {
      link: join(ROUTE_PREFIX, `/node/${nodeID}/snapshot/${snapshotID}`),
      name: `Node ${nodeID}, Snapshot ${snapshotID}`,
    },
    {
      link: join(
        ROUTE_PREFIX,
        `/node/${nodeID}/snapshot/${snapshotID}/span/${spanID}`,
      ),
      name: `${span?.operation}`,
    },
  ];
  return !spanID ? (
    <SnapshotComponent {...snapProps} />
  ) : !isRaw ? (
    <>
      <Breadcrumbs items={breadcrumbItems} />
      <SpanComponent {...spanProps} />
    </>
  ) : (
    <>
      <Breadcrumbs
        items={[
          ...breadcrumbItems,
          {
            link: join(
              ROUTE_PREFIX,
              `/node/${nodeID}/snapshot/${snapshotID}/span/${spanID}/raw`,
            ),
            name: `raw trace`,
          },
        ]}
      />
      <RawTraceComponent {...rawTraceProps} />
    </>
  );
};
