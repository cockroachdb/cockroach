// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";

import { AxisProps } from "src/views/shared/components/metricQuery";

import { CustomMetricState } from "../customChart/customMetric";

/**
 * DashboardConfig is the configuration for a dashboard.
 */
export interface DashboardConfig {
  key: React.Key;
  name: string; // File name.
  graphs: GraphConfig[];
}

export interface GraphConfig {
  title: string;
  axis: AxisProps;
  metrics: CustomMetricState[];
}

/**
 * Generates a default title from metric names, capped at 3 metrics.
 * Uses the full metric name.
 */
export function generateGraphTitleFromMetrics(
  metrics: CustomMetricState[],
): string {
  const validMetrics = metrics.filter(m => m.metric && m.metric.trim() !== "");
  if (validMetrics.length === 0) {
    return "";
  }

  const metricNames = validMetrics.slice(0, 3).map(m => m.metric);

  if (validMetrics.length > 3) {
    return `${metricNames.join(", ")} (+${validMetrics.length - 3} more)`;
  }
  return metricNames.join(", ");
}
