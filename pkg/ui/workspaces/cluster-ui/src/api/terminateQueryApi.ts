// Copyright 2021 The Cockroach Authors.
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

const STATUS_PREFIX = "/_status";

export type CancelSessionRequestMessage = cockroach.server.serverpb.CancelSessionRequest;
export type CancelSessionResponseMessage = cockroach.server.serverpb.CancelSessionResponse;
export type CancelQueryRequestMessage = cockroach.server.serverpb.CancelQueryRequest;
export type CancelQueryResponseMessage = cockroach.server.serverpb.CancelQueryResponse;

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
