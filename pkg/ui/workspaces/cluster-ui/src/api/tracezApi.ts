// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { fetchData } from "src/api";
import TakeTracingSnapshotRequest = cockroach.server.serverpb.TakeTracingSnapshotRequest;

export type ListTracingSnapshotsRequestMessage =
  cockroach.server.serverpb.ListTracingSnapshotsRequest;
export type ListTracingSnapshotsResponseMessage =
  cockroach.server.serverpb.ListTracingSnapshotsResponse;

export type TakeTracingSnapshotRequestMessage = TakeTracingSnapshotRequest;
export type TakeTracingSnapshotResponseMessage =
  cockroach.server.serverpb.TakeTracingSnapshotResponse;

export type GetTracingSnapshotRequestMessage =
  cockroach.server.serverpb.GetTracingSnapshotRequest;
export type GetTracingSnapshotResponseMessage =
  cockroach.server.serverpb.GetTracingSnapshotResponse;

export type Span = cockroach.server.serverpb.ITracingSpan;
export type Snapshot = cockroach.server.serverpb.ITracingSnapshot;

export type GetTraceRequestMessage = cockroach.server.serverpb.GetTraceRequest;
export type GetTraceResponseMessage =
  cockroach.server.serverpb.GetTraceResponse;

const API_PREFIX = "_admin/v1";

const proxyNonLocalNode = (path: string, nodeID: string): string => {
  if (nodeID === "local") {
    // While the server is clever enough to do the smart thing around proxying to node
    // "local," it still queries gossip while doing it. We'd like to avoid a hard dependency
    // on that to support malfunctioning clusters or nodes.
    return path;
  }
  return path + `?remote_node_id=${nodeID}`;
};

export function listTracingSnapshots(
  nodeID: string,
): Promise<ListTracingSnapshotsResponseMessage> {
  return fetchData(
    cockroach.server.serverpb.ListTracingSnapshotsResponse,
    proxyNonLocalNode(`${API_PREFIX}/trace_snapshots`, nodeID),
    null,
    null,
  );
}

export function takeTracingSnapshot(
  nodeID: string,
): Promise<TakeTracingSnapshotResponseMessage> {
  const req = new TakeTracingSnapshotRequest();
  return fetchData(
    cockroach.server.serverpb.TakeTracingSnapshotResponse,
    proxyNonLocalNode(`${API_PREFIX}/trace_snapshots`, nodeID),
    cockroach.server.serverpb.TakeTracingSnapshotRequest,
    req as any,
  );
}

export function getTracingSnapshot(req: {
  nodeID: string;
  snapshotID: number;
}): Promise<GetTracingSnapshotResponseMessage> {
  return fetchData(
    cockroach.server.serverpb.GetTracingSnapshotResponse,
    proxyNonLocalNode(
      `${API_PREFIX}/trace_snapshots/${req.snapshotID}`,
      req.nodeID,
    ),
    null,
    null,
  );
}

export function getTraceForSnapshot(req: {
  nodeID: string;
  req: GetTraceRequestMessage;
}): Promise<GetTraceResponseMessage> {
  return fetchData(
    cockroach.server.serverpb.GetTraceResponse,
    proxyNonLocalNode(`${API_PREFIX}/traces`, req.nodeID),
    cockroach.server.serverpb.GetTraceRequest,
    req.req as any,
  );
}
