// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";

import { useSwrImmutableWithClusterId } from "../util";

import { fetchData } from "./fetchData";

type MetricMetadataResponse =
  cockroach.server.serverpb.MetricMetadataResponse;

const METRIC_METADATA_SWR_KEY = "metricMetadata";

const getMetricMetadata = (): Promise<MetricMetadataResponse> => {
  return fetchData(
    cockroach.server.serverpb.MetricMetadataResponse,
    "_admin/v1/metricmetadata",
  );
};

export const useMetricMetadata = () => {
  const { data, error, isLoading } = useSwrImmutableWithClusterId(
    METRIC_METADATA_SWR_KEY,
    getMetricMetadata,
  );

  return { data, isLoading, error };
};
