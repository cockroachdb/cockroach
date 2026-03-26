// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";

import { fetchData } from "src/api";
import { useSwrWithClusterId } from "src/util";

const SESSIONS_PATH = "_status/sessions";
const SESSIONS_KEY = "sessions";

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

export interface UseSessionsOptions {
  excludeClosedSessions?: boolean;
  refreshInterval?: number;
  // When true, uses cached data without revalidating. Useful for
  // detail pages where a revalidation would likely return different
  // results, causing the viewed item to disappear.
  immutable?: boolean;
  // Called after each successful fetch with the new data.
  onSuccess?: (data: SessionsResponseMessage) => void;
}

export const useSessions = (opts?: UseSessionsOptions) => {
  const immutableConfig = opts?.immutable
    ? {
        revalidateIfStale: false,
        revalidateOnFocus: false,
        revalidateOnReconnect: false,
      }
    : {};

  const { data, isLoading, error, mutate } =
    useSwrWithClusterId<SessionsResponseMessage>(
      {
        name: SESSIONS_KEY,
        excludeClosedSessions: opts?.excludeClosedSessions ?? false,
      },
      () => getSessions({ excludeClosedSessions: opts?.excludeClosedSessions }),
      {
        revalidateOnFocus: false,
        dedupingInterval: 5_000,
        refreshInterval: opts?.refreshInterval,
        onSuccess: opts?.onSuccess,
        ...immutableConfig,
      },
    );

  return {
    data: data ?? null,
    isLoading,
    error: error ?? null,
    refresh: mutate,
  };
};
