// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { AxisUnits } from "@cockroachlabs/cluster-ui";
import map from "lodash/map";
import React from "react";

import LineGraph from "src/views/cluster/components/linegraph";
import { Metric, Axis } from "src/views/shared/components/metricQuery";

import { GraphDashboardProps } from "./dashboardUtils";

export default function (props: GraphDashboardProps) {
  const { nodeSources, tenantSource } = props;

  const percentiles = ["p50", "p75", "p90", "p99"];

  return [
    <LineGraph
      title="Processing Rate"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
    >
      <Axis label="rows per second" units={AxisUnits.Count}>
        <Metric
          name="cr.node.jobs.row_level_ttl.rows_selected"
          title="rows selected"
          nonNegativeRate
        />
        <Metric
          name="cr.node.jobs.row_level_ttl.rows_deleted"
          title="rows deleted"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,
    <LineGraph
      title="Estimated Rows"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
    >
      <Axis label="row count" units={AxisUnits.Count}>
        <Metric
          name="cr.node.jobs.row_level_ttl.total_rows"
          title="approximate number of rows"
          nonNegativeRate
        />
        <Metric
          name="cr.node.jobs.row_level_ttl.total_expired_rows"
          title="approximate number of expired rows"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,
    <LineGraph
      title="Job Latency"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`Latency of scanning and deleting within the job.`}
      showMetricsInTooltip={true}
    >
      <Axis label="latency" units={AxisUnits.Duration}>
        {map(percentiles, p => (
          <>
            <Metric
              name={`cr.node.jobs.row_level_ttl.select_duration-${p}`}
              title={`scan latency (${p})`}
              downsampleMax
            />
            <Metric
              name={`cr.node.jobs.row_level_ttl.delete_duration-${p}`}
              title={`delete latency (${p})`}
              downsampleMax
            />
          </>
        ))}
      </Axis>
    </LineGraph>,
    <LineGraph
      title="Spans in Progress"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`Number of active spans being processed by TTL.`}
      showMetricsInTooltip={true}
    >
      <Axis label="span count" units={AxisUnits.Count}>
        <Metric
          name="cr.node.jobs.row_level_ttl.num_active_spans"
          title="number of spans being processed"
        />
      </Axis>
    </LineGraph>,
  ];
}
