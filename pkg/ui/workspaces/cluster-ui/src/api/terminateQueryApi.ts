// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";

import { fetchData } from "src/api";

const STATUS_PREFIX = "_status";

export type CancelSessionRequestMessage =
  cockroach.server.serverpb.ICancelSessionRequest;
export type CancelSessionResponseMessage =
  cockroach.server.serverpb.CancelSessionResponse;
export type CancelQueryRequestMessage =
  cockroach.server.serverpb.ICancelQueryRequest;
export type CancelQueryResponseMessage =
  cockroach.server.serverpb.CancelQueryResponse;

export const terminateSession = (
  req: CancelSessionRequestMessage,
): Promise<CancelSessionResponseMessage> => {
  return fetchData(
    cockroach.server.serverpb.CancelSessionResponse,
    `${STATUS_PREFIX}/cancel_session/${req.node_id}`,
    cockroach.server.serverpb.CancelSessionRequest,
    req,
  );
};

export const terminateQuery = (
  req: CancelQueryRequestMessage,
): Promise<CancelQueryResponseMessage> => {
  return fetchData(
    cockroach.server.serverpb.CancelQueryResponse,
    `${STATUS_PREFIX}/cancel_query/${req.node_id}`,
    cockroach.server.serverpb.CancelQueryRequest,
    req,
  );
};
