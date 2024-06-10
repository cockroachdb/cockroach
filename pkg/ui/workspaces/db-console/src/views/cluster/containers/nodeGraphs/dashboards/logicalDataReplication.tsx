// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { AxisUnits, util } from "@cockroachlabs/cluster-ui";

import LineGraph from "src/views/cluster/components/linegraph";
import { Metric, Axis } from "src/views/shared/components/metricQuery";
import { cockroach } from "src/js/protos";

import { GraphDashboardProps, nodeDisplayName } from "./dashboardUtils";

import TimeSeriesQueryAggregator = cockroach.ts.tspb.TimeSeriesQueryAggregator;

export default function (props: GraphDashboardProps) {
  const { storeSources, tenantSource, nodeIDs,nodeDisplayNameByID} = props;

  return [
    <LineGraph
      title="Logical Bytes Received"
      sources={storeSources}
      tenantSource={tenantSource}
      tooltip={`Rate at which the logical bytes (sum of keys + values) are received by all logical replication jobs`}
    >
      <Axis units={AxisUnits.Bytes} label="bytes">
      {nodeIDs.map( node => (
      <Metric
        key={node}
        name="cr.node.logical_replication.logical_bytes"
        title={nodeDisplayName(nodeDisplayNameByID, node)}
        sources={[node]}
        nonNegativeRate
      />
    ))}
      </Axis>
    </LineGraph>,

     <LineGraph
     title="Row Updates Applied"
     sources={storeSources}
     tenantSource={tenantSource}
     tooltip={`Rate at which row updates are applied by all logical replication jobs`}
   >
     <Axis label="updates">
       <Metric
         name="cr.node.logical_replication.events_ingested"
         title="Row Updates Applied"
         nonNegativeRate
       />
     </Axis>
   </LineGraph>,

    <LineGraph
    title="Age of Applied Row Updates"
    isKvGraph={false}
    tenantSource={tenantSource}
    tooltip={"The difference in row update commit times between the source cluster and the destination cluster"}
    showMetricsInTooltip={true}
  >
    <Axis units={AxisUnits.Duration} label="latency">
        <Metric
        name="cr.node.logical_replication.commit_latency-p50"
        title={"p50"}
        downsampleMax
        />
        <Metric
        name="cr.node.logical_replication.commit_latency-p95"
        title={"p90"}
        downsampleMax
        />
        <Metric
        name="cr.node.logical_replication.commit_latency-p99"
        title={"p99"}
        downsampleMax
      />
    </Axis>
  </LineGraph>,

  <LineGraph
    title="Age of Oldest Row Pending Replication"
    sources={storeSources}
    tenantSource={tenantSource}
    tooltip={`Age of Oldest Row on Source that has yet to replicate to destination cluster`}
  >
    <Axis units={AxisUnits.Duration} label="duration">
      <Metric
        downsampler={TimeSeriesQueryAggregator.MIN}
        aggregator={TimeSeriesQueryAggregator.MAX}
        name="cr.node.logical_replication.replicated_time_seconds"
        title="Age of Oldest Row Pending Replication"
        transform={datapoints =>
          datapoints
            .filter(d => d.value !== 0)
            .map(d =>
              d.value
                ? {
                    ...d,
                    value:
                      d.timestamp_nanos.toNumber() -
                      util.SecondsToNano(d.value),
                  }
                : d,
            )
        }
      />
    </Axis>
  </LineGraph>,

  <LineGraph
  title="Batch Application Processing Time: 50th percentile"
  isKvGraph={false}
  tenantSource={tenantSource}
  tooltip={"The 50th percentile in the time it takes to write a batch of row updates"}
  showMetricsInTooltip={true}
  >
  <Axis units={AxisUnits.Duration} label="processing time">
    {nodeIDs.map(node => (
      <Metric
        key={node}
        name="cr.node.logical_replication.batch_hist_nanos-p50"
        title={nodeDisplayName(nodeDisplayNameByID, node)}
        sources={[node]}
        downsampleMax
      />
    ))}
  </Axis>
  </LineGraph>,

  <LineGraph
  title="Batch Application Processing Time: 99th percentile"
  isKvGraph={false}
  tenantSource={tenantSource}
  tooltip={"The 99th percentile in the time it takes to write a batch of row updates"}
  showMetricsInTooltip={true}
  >
  <Axis units={AxisUnits.Duration} label="processing time">
  {nodeIDs.map( node => (
    <Metric
      key={node}
      name="cr.node.logical_replication.batch_hist_nanos-p99"
      title={nodeDisplayName(nodeDisplayNameByID, node)}
      sources={[node]}
      downsampleMax
    />
  ))}
  </Axis>
  </LineGraph>,
  ];
}
