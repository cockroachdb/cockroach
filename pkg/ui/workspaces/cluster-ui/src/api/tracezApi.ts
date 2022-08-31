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

export function listTracingSnapshots(): Promise<ListTracingSnapshotsResponseMessage> {
  return fetchData(
    cockroach.server.serverpb.ListTracingSnapshotsResponse,
    `${API_PREFIX}/trace_snapshots`,
    null,
    null,
  );
}

export function takeTracingSnapshot(): Promise<TakeTracingSnapshotResponseMessage> {
  const req = new TakeTracingSnapshotRequest();
  return fetchData(
    cockroach.server.serverpb.TakeTracingSnapshotResponse,
    `${API_PREFIX}/trace_snapshots`,
    req as any,
    null,
  );
}

export function getTracingSnapshot(
  snapshotID: number,
): Promise<GetTracingSnapshotResponseMessage> {
  return fetchData(
    cockroach.server.serverpb.GetTracingSnapshotResponse,
    `${API_PREFIX}/trace_snapshots/${snapshotID}`,
    null,
    null,
  );
}

export function getTraceForSnapshot(
  req: GetTraceRequestMessage,
): Promise<GetTraceResponseMessage> {
  return fetchData(
    cockroach.server.serverpb.GetTraceResponse,
    `${API_PREFIX}/traces`,
    req as any,
    null,
  );
}
