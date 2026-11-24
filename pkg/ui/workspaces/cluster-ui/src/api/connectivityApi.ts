// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";

import { fetchData } from "src/api";

import { useSwrWithClusterId } from "../util";

const CONNECTIVITY_PATH = "_status/connectivity";

export const CONNECTIVITY_SWR_KEY = "connectivity";

export const getConnectivity =
  (): Promise<cockroach.server.serverpb.NetworkConnectivityResponse> => {
    return fetchData(
      cockroach.server.serverpb.NetworkConnectivityResponse,
      CONNECTIVITY_PATH,
    );
  };

export const useConnectivity = () => {
  const { data, isLoading, error } = useSwrWithClusterId(
    CONNECTIVITY_SWR_KEY,
    getConnectivity,
    {
      revalidateOnFocus: false,
      dedupingInterval: 10000, // 10 seconds.
    },
  );

  return {
    isLoading,
    error,
    connections: data?.connections ?? {},
  };
};
