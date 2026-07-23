// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";

import { fetchData } from "src/api";

import { useSwrWithClusterId } from "../util";

type LogEntriesResponse = cockroach.server.serverpb.LogEntriesResponse;

const NODE_LOGS_KEY = "nodeLogs";

function getNodeLogs(nodeId: string): Promise<LogEntriesResponse> {
  return fetchData(
    cockroach.server.serverpb.LogEntriesResponse,
    `_status/logs/${nodeId}`,
  );
}

export const useNodeLogs = (nodeId: string) => {
  const shouldFetch = Boolean(nodeId);
  const { data, isLoading, error } = useSwrWithClusterId(
    shouldFetch ? { name: NODE_LOGS_KEY, nodeId } : null,
    shouldFetch ? () => getNodeLogs(nodeId) : null,
    {
      revalidateOnFocus: false,
      dedupingInterval: 10_000,
    },
  );

  return {
    data,
    isLoading,
    error,
  };
};
