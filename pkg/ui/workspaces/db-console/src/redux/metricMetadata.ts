// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { createSelector } from "reselect";

import { AdminUIState } from "src/redux/state";
import { MetricMetadataResponseMessage } from "src/util/api";

export type MetricsMetadata = MetricMetadataResponseMessage["metadata"];

// State selectors
const metricsMetadataStateSelector = (state: AdminUIState) =>
  state.cachedData.metricMetadata.data;

export const metricsMetadataSelector = createSelector(
  metricsMetadataStateSelector,
  (metricsMetadata): MetricsMetadata =>
    metricsMetadata ? metricsMetadata.metadata : undefined,
);
