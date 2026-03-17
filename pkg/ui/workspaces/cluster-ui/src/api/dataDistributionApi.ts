// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";

import { useSwrWithClusterId } from "../util";

import { fetchData } from "./fetchData";

type DataDistributionResponse =
  cockroach.server.serverpb.DataDistributionResponse;

const DATA_DISTRIBUTION_PATH = "_admin/v1/data_distribution";

export const DATA_DISTRIBUTION_SWR_KEY = "dataDistribution";

const getDataDistribution = (): Promise<DataDistributionResponse> => {
  return fetchData(
    cockroach.server.serverpb.DataDistributionResponse,
    DATA_DISTRIBUTION_PATH,
  );
};

export const useDataDistribution = () => {
  const { data, isLoading, error } = useSwrWithClusterId(
    DATA_DISTRIBUTION_SWR_KEY,
    getDataDistribution,
    {
      revalidateOnFocus: false,
      dedupingInterval: 60000, // 1 minute.
    },
  );

  return {
    data,
    isLoading,
    error,
  };
};
