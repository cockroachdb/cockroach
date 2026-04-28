// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";

import { useSwrImmutableWithClusterId } from "../util";

import { fetchData } from "./fetchData";

type ListTenantsResponse =
  cockroach.server.serverpb.ListTenantsResponse;

const TENANTS_SWR_KEY = "tenants";

const getTenants = (): Promise<ListTenantsResponse> => {
  return fetchData(
    cockroach.server.serverpb.ListTenantsResponse,
    "_admin/v1/tenants",
  );
};

export const useTenants = (opts?: { enabled?: boolean }) => {
  const enabled = opts?.enabled ?? true;
  const { data, error, isLoading } = useSwrImmutableWithClusterId(
    enabled ? TENANTS_SWR_KEY : null,
    getTenants,
  );

  return {
    tenants: data?.tenants ?? [],
    isLoading,
    error,
  };
};
