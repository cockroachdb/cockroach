// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { useContext } from "react";

import { fetchData } from "src/api";

import { ClusterDetailsContext } from "../contexts";
import { useSwrWithClusterId } from "../util";

const LIVENESS_PATH = "_admin/v1/liveness";

export const LIVENESS_SWR_KEY = "liveness";

export const getLiveness =
  (): Promise<cockroach.server.serverpb.LivenessResponse> => {
    return fetchData(cockroach.server.serverpb.LivenessResponse, LIVENESS_PATH);
  };

export const useLiveness = () => {
  const { isTenant } = useContext(ClusterDetailsContext);
  const { data, isLoading, error } = useSwrWithClusterId(
    LIVENESS_SWR_KEY,
    !isTenant ? getLiveness : null,
    {
      revalidateOnFocus: false,
      dedupingInterval: 10000, // 10 seconds.
    },
  );

  return {
    isLoading,
    error,
    livenesses: data?.livenesses ?? [],
    statuses: data?.statuses ?? {},
  };
};
