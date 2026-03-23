// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";

import { fetchData } from "src/api";

const SESSIONS_PATH = "_status/sessions";

export type SessionsRequestMessage =
  cockroach.server.serverpb.ListSessionsRequest;
export type SessionsResponseMessage =
  cockroach.server.serverpb.ListSessionsResponse;

export interface SessionsRequest {
  excludeClosedSessions?: boolean;
}

// getSessions gets all cluster sessions.
export const getSessions = (
  req?: SessionsRequest,
): Promise<SessionsResponseMessage> => {
  const params = new URLSearchParams();
  if (req?.excludeClosedSessions) {
    params.set("exclude_closed_sessions", "true");
  }
  const path =
    params.toString() !== ""
      ? `${SESSIONS_PATH}?${params.toString()}`
      : SESSIONS_PATH;
  return fetchData(cockroach.server.serverpb.ListSessionsResponse, path);
};
