// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createSelector } from "reselect";

import { AdminUIState } from "src/redux/state";
import { MetricMetadataResponseMessage } from "src/util/api";

import { DropdownOption } from "../views/shared/components/dropdown";

export type MetricsMetadata = MetricMetadataResponseMessage;

// State selectors
const metricsMetadataStateSelector = (state: AdminUIState) =>
  state.cachedData.metricMetadata.data;

export const metricsMetadataSelector = createSelector(
  metricsMetadataStateSelector,
  (metricsMetadata): MetricsMetadata => metricsMetadata,
);

export const metricOptionsSelector = createSelector(
  metricsMetadataSelector,
  (metricsMetadata): DropdownOption[] => {
    if (metricsMetadata?.metadata == null) {
      return [];
    }

    return Object.keys(metricsMetadata.metadata).map(k => {
      const fullMetricName = metricsMetadata.recordedNames[k];
      return {
        value: fullMetricName,
        label: k,
        description: metricsMetadata.metadata[k]?.help,
      };
    });
  },
);
