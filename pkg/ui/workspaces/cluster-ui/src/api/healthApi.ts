// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";

import { fetchData } from "src/api";

import { useSwrWithClusterId } from "../util";

const HEALTH_PATH = "_admin/v1/health";
const HEALTH_SWR_KEY = "health";

const getHealth = (): Promise<cockroach.server.serverpb.HealthResponse> => {
  return fetchData(cockroach.server.serverpb.HealthResponse, HEALTH_PATH);
};

export const useHealth = () => {
  const { data, error, isLoading } = useSwrWithClusterId(
    HEALTH_SWR_KEY,
    getHealth,
    {
      refreshInterval: 30_000,
      revalidateOnFocus: false,
      dedupingInterval: 30_000,
    },
  );
  return { data, error, isLoading };
};
