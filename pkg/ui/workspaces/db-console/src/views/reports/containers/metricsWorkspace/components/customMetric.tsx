// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";

import { MetricsMetadata } from "oss/src/redux/metricMetadata";
import { NodesSummary } from "oss/src/redux/nodes";
import { Metric } from "oss/src/views/shared/components/metricQuery";
import { CustomMetricState } from "src/views/reports/containers/customChart/customMetric";

import { getSources } from "../../customChart";

type Props = {
  key: React.Key;
  metric: CustomMetricState;
  tenants: string[];
  nodesSummary: NodesSummary;
  metricsMetadata: MetricsMetadata;
};

const CustomMetric = ({
  key,
  metric,
  nodesSummary,
  tenants,
  metricsMetadata,
}: Props): React.ReactElement => {
  const sources = getSources(nodesSummary, metric, metricsMetadata);
  if (metric.perSource && metric.perTenant) {
    return (
      <React.Fragment>
        {sources.flatMap(source => {
          return tenants.map((tenant: string, i: number) => (
            <Metric
              key={`${key}${i}${source}${tenant}`}
              title={`${source}-${tenant}: ${metric.metric} (${i})`}
              name={metric.metric}
              aggregator={metric.aggregator}
              downsampler={metric.downsampler}
              derivative={metric.derivative}
              sources={[source]}
              tenantSource={tenant}
            />
          ));
        })}
      </React.Fragment>
    );
  }

  if (metric.perSource) {
    return (
      <React.Fragment>
        {sources.map((source: string, i: number) => (
          <Metric
            key={`${key}${i}${source}`}
            title={`${source}: ${metric.metric} (${i})`}
            name={metric.metric}
            aggregator={metric.aggregator}
            downsampler={metric.downsampler}
            derivative={metric.derivative}
            sources={[source]}
            tenantSource={metric.tenantSource}
          />
        ))}
      </React.Fragment>
    );
  }

  if (metric.perTenant) {
    return (
      <React.Fragment>
        {tenants.map((tenant: string, i: number) => (
          <Metric
            key={`${key}${i}${tenant}`}
            title={`${tenant}: ${metric.metric} (${i})`}
            name={metric.metric}
            aggregator={metric.aggregator}
            downsampler={metric.downsampler}
            derivative={metric.derivative}
            sources={sources}
            tenantSource={tenant}
          />
        ))}
      </React.Fragment>
    );
  }

  return (
    <Metric
      key={key}
      title={`${metric.metric}`}
      name={metric.metric}
      aggregator={metric.aggregator}
      downsampler={metric.downsampler}
      derivative={metric.derivative}
      sources={sources}
      tenantSource={metric.tenantSource}
    />
  );
};

export default CustomMetric;
