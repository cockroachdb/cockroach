// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";

import { useSwrWithClusterId } from "../util";

import { fetchData } from "./fetchData";

type ClusterResponse = cockroach.server.serverpb.ClusterResponse;

const CLUSTER_SWR_KEY = "cluster";

const getCluster = (): Promise<ClusterResponse> => {
  return fetchData(
    cockroach.server.serverpb.ClusterResponse,
    "_admin/v1/cluster",
  );
};

export const useCluster = () => {
  const { data, isLoading, error } = useSwrWithClusterId(
    CLUSTER_SWR_KEY,
    getCluster,
    {
      revalidateOnFocus: false,
      dedupingInterval: 10_000,
    },
  );

  return {
    data,
    isLoading,
    error,
    enterpriseEnabled: data?.enterprise_enabled ?? false,
  };
};
