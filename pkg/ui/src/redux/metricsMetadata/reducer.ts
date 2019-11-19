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

import {
  METRICS_METADATA_REQUEST,
  METRICS_METADATA_REQUEST_COMPLETE,
  METRICS_METADATA_REQUEST_ERROR,
  MetricsMetadataAction,
} from "./actions";

export interface MetricsMetadataState {
  metadata: MetricMetadataResponseMessage;
  error: Error;
}

const initialState: MetricsMetadataState = {
  metadata: null,
  error: null,
};

export function metricsMetadataReducer(
  state: MetricsMetadataState = initialState,
  action: MetricsMetadataAction): MetricsMetadataState {
  switch (action.type) {
    case METRICS_METADATA_REQUEST: {
      return {
        ...state,
      };
    }
    case METRICS_METADATA_REQUEST_ERROR: {
      return {
        ...state,
        error: action.payload,
      };
    }
    case METRICS_METADATA_REQUEST_COMPLETE: {
      return {
        metadata: action.payload,
        error: null,
      };
    }
    default:
      return state;
  }
}
