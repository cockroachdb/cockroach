// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { MetricMetadataResponseMessage } from "src/util/api";

export const METRICS_METADATA_REQUEST = "cockroachui/metricsMetadata/REQUEST";
export const METRICS_METADATA_REQUEST_ERROR = "cockroachui/metricsMetadata/REQUEST_ERROR";
export const METRICS_METADATA_REQUEST_COMPLETE = "cockroachui/metricsMetadata/REQUEST_COMPLETE";

type MetricsMetadataErrorAction = {
  type: typeof METRICS_METADATA_REQUEST_ERROR;
  payload: Error;
};

type MetricsMetadataReceivedAction = {
  type: typeof METRICS_METADATA_REQUEST_COMPLETE;
  payload: MetricMetadataResponseMessage
};

type MetricsMetadataRequestAction = {
  type: typeof METRICS_METADATA_REQUEST;
};

export type MetricsMetadataAction =
  | MetricsMetadataRequestAction
  | MetricsMetadataReceivedAction
  | MetricsMetadataErrorAction;

const request = (): MetricsMetadataRequestAction => {
  return {
    type: METRICS_METADATA_REQUEST,
  };
};

const requestCompleted = (metrics: MetricMetadataResponseMessage): MetricsMetadataReceivedAction => {
  return {
    type: METRICS_METADATA_REQUEST_COMPLETE,
    payload: metrics,
  };
};

const requestError = (error: Error): MetricsMetadataErrorAction => {
  return {
    type: METRICS_METADATA_REQUEST_ERROR,
    payload: error,
  };
};

export const metricsMetadataActions = {
  request,
  requestCompleted,
  requestError,
};
