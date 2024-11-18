// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import isEmpty from "lodash/isEmpty";
import { createSelector } from "reselect";

import { nodeStatusSelector } from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import { MetricMetadataResponseMessage } from "src/util/api";

export type MetricsMetadata = {
  metadata?: MetricMetadataResponseMessage["metadata"];
  allMetrics?: Set<string>;
  storeMetrics?: Set<string>;
};

// State selectors
const metricsMetadataStateSelector = (state: AdminUIState) =>
  state.cachedData.metricMetadata.data;

export const metricsMetadataSelector = createSelector(
  metricsMetadataStateSelector,
  nodeStatusSelector,
  (metricsMetadata, nodeStatus): MetricsMetadata => {
    if (isEmpty(metricsMetadata) || isEmpty(nodeStatus)) {
      return {};
    }
    return {
      metadata: metricsMetadata?.metadata,
      allMetrics: new Set(Object.keys(nodeStatus?.metrics ?? {})),
      storeMetrics: new Set(
        Object.keys(nodeStatus?.store_statuses[0].metrics ?? {}),
      ),
    };
  },
);
