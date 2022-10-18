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

export type ListTracingSnapshotsRequest =
  cockroach.server.serverpb.ListTracingSnapshotsRequest;
export type ListTracingSnapshotsResponse =
  cockroach.server.serverpb.ListTracingSnapshotsResponse;

export const TakeTracingSnapshotRequest =
  cockroach.server.serverpb.TakeTracingSnapshotRequest;
export type TakeTracingSnapshotResponse =
  cockroach.server.serverpb.TakeTracingSnapshotResponse;

export type GetTracingSnapshotRequest =
  cockroach.server.serverpb.GetTracingSnapshotRequest;
export type GetTracingSnapshotResponse =
  cockroach.server.serverpb.GetTracingSnapshotResponse;

export type Span = cockroach.server.serverpb.ITracingSpan;
export type Snapshot = cockroach.server.serverpb.ITracingSnapshot;

export type GetTraceRequest = cockroach.server.serverpb.GetTraceRequest;
export type GetTraceResponse = cockroach.server.serverpb.GetTraceResponse;

const API_PREFIX = "_admin/v1";

export function listTracingSnapshots(
  nodeID: string,
): Promise<ListTracingSnapshotsResponse> {
  // Note that the server is clever enough to ignore proxy requests to node "local."
  return fetchData(
    cockroach.server.serverpb.ListTracingSnapshotsResponse,
    `${API_PREFIX}/trace_snapshots?remote_node_id=${nodeID}`,
    null,
    null,
  );
}

export function takeTracingSnapshot(
  nodeID: string,
): Promise<TakeTracingSnapshotResponse> {
  const req = new TakeTracingSnapshotRequest();
  return fetchData(
    cockroach.server.serverpb.TakeTracingSnapshotResponse,
    `${API_PREFIX}/trace_snapshots?remote_node_id=${nodeID}`,
    cockroach.server.serverpb.TakeTracingSnapshotRequest,
    req as any,
  );
}

export function getTracingSnapshot(req: {
  nodeID: string;
  snapshotID: number;
}): Promise<GetTracingSnapshotResponse> {
  return fetchData(
    cockroach.server.serverpb.GetTracingSnapshotResponse,
    `${API_PREFIX}/trace_snapshots/${req.snapshotID}?remote_node_id=${req.nodeID}`,
    null,
    null,
  );
}

export function getTraceForSnapshot(req: {
  nodeID: string;
  req: GetTraceRequest;
}): Promise<GetTraceResponse> {
  return fetchData(
    cockroach.server.serverpb.GetTraceResponse,
    `${API_PREFIX}/traces?remote_node_id=${req.nodeID}`,
    cockroach.server.serverpb.GetTraceRequest,
    req.req as any,
  );
}
