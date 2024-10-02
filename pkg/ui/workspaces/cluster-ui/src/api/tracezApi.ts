// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import Long from "long";

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

export type NamedOperationMetadata =
  cockroach.server.serverpb.INamedOperationMetadata;
export type Span = cockroach.server.serverpb.ITracingSpan;
export type Snapshot = cockroach.server.serverpb.ITracingSnapshot;

export const GetTraceRequest = cockroach.server.serverpb.GetTraceRequest;
export type GetTraceResponse = cockroach.server.serverpb.GetTraceResponse;

export const SetTraceRecordingTypeRequest =
  cockroach.server.serverpb.SetTraceRecordingTypeRequest;
export type SetTraceRecordingTypeResponse =
  cockroach.server.serverpb.SetTraceRecordingTypeResponse;
export type RecordingMode = cockroach.util.tracing.tracingpb.RecordingMode;

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
    req,
  );
}

// This is getting plugged into our redux libraries, which want calls with a
// single argument. So wrap the two arguments in a request object.
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

// This is getting plugged into our redux libraries, which want calls with a
// single argument. So wrap the two arguments in a request object.
export function getRawTrace(req: {
  nodeID: string;
  snapshotID: number;
  traceID: Long;
}): Promise<GetTraceResponse> {
  const rpcReq = new GetTraceRequest({
    snapshot_id: Long.fromNumber(req.snapshotID),
    trace_id: req.traceID,
  });
  return fetchData(
    cockroach.server.serverpb.GetTraceResponse,
    `${API_PREFIX}/traces?remote_node_id=${req.nodeID}`,
    cockroach.server.serverpb.GetTraceRequest,
    rpcReq,
  );
}

export function setTraceRecordingType(
  nodeID: string,
  traceID: Long,
  recordingMode: RecordingMode,
): Promise<SetTraceRecordingTypeResponse> {
  const req = new SetTraceRecordingTypeRequest({
    trace_id: traceID,
    recording_mode: recordingMode,
  });
  return fetchData(
    cockroach.server.serverpb.SetTraceRecordingTypeResponse,
    // TODO(davidh): Consider making this endpoint just POST to `/traces/{trace_ID}`
    `${API_PREFIX}/settracerecordingtype?remote_node_id=${nodeID}`,
    cockroach.server.serverpb.SetTraceRecordingTypeRequest,
    req,
  );
}
