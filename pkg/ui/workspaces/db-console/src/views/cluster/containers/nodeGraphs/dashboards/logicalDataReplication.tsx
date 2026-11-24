// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { AxisUnits, util } from "@cockroachlabs/cluster-ui";
import React from "react";

import { cockroach } from "src/js/protos";
import LineGraph from "src/views/cluster/components/linegraph";
import { Metric, Axis } from "src/views/shared/components/metricQuery";

import { GraphDashboardProps, nodeDisplayName } from "./dashboardUtils";

import TimeSeriesQueryAggregator = cockroach.ts.tspb.TimeSeriesQueryAggregator;

export default function (props: GraphDashboardProps) {
  const { storeSources, tenantSource, nodeIDs, nodeDisplayNameByID } = props;

  return [
    <LineGraph
      title="Replication Latency"
      isKvGraph={false}
      tenantSource={tenantSource}
      tooltip={
        "The difference in commit times between the source cluster and the destination cluster"
      }
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        <Metric
          name="cr.node.logical_replication.commit_latency-p50"
          title={"p50"}
          aggregateMax
          downsampleMax
        />
        <Metric
          name="cr.node.logical_replication.commit_latency-p99"
          title={"p99"}
          aggregateMax
          downsampleMax
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Replication Lag"
      sources={storeSources}
      tenantSource={tenantSource}
      tooltip={
        "The age of the oldest row on the source cluster that has yet to replicate to destination cluster"
      }
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Duration} label="duration">
        <Metric
          downsampler={TimeSeriesQueryAggregator.MIN}
          aggregator={TimeSeriesQueryAggregator.MAX}
          name="cr.node.logical_replication.replicated_time_seconds"
          title="Replication Lag"
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
      title="Row Updates Applied"
      sources={storeSources}
      tenantSource={tenantSource}
      tooltip={`Rate at which row updates are applied by all logical replication jobs`}
      showMetricsInTooltip={true}
    >
      <Axis label="updates">
        <Metric
          name="cr.node.logical_replication.events_ingested"
          title="Row Updates Applied"
          nonNegativeRate
        />
        <Metric
          name="cr.node.logical_replication.events_dlqed"
          title="Row Updates sent to DLQ"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Logical Bytes Received"
      sources={storeSources}
      tenantSource={tenantSource}
      tooltip={`Rate at which the logical bytes (sum of keys + values) are received by all logical replication jobs`}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Bytes} label="bytes">
        {nodeIDs.map(node => (
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
      title="Row Application Processing Time: 50th percentile"
      isKvGraph={false}
      tenantSource={tenantSource}
      tooltip={
        "The 50th percentile in the time it takes to write a batch of row updates"
      }
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
      title="Row Application Processing Time: 99th percentile"
      isKvGraph={false}
      tenantSource={tenantSource}
      tooltip={
        "The 99th percentile in the time it takes to write a batch of row updates"
      }
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Duration} label="processing time">
        {nodeIDs.map(node => (
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

    <LineGraph
      title="DLQ Causes"
      sources={storeSources}
      tenantSource={tenantSource}
      tooltip={`Reasons events were sent to the DLQ `}
      showMetricsInTooltip={true}
    >
      <Axis label="updates">
        <Metric
          name="cr.node.logical_replication.events_dlqed_age"
          title="Retry Duration Expired"
          nonNegativeRate
        />
        <Metric
          name="cr.node.logical_replication.events_dlqed_space"
          title="Retry Queue Full"
          nonNegativeRate
        />
        <Metric
          name="cr.node.logical_replication.events_dlqed_errtype"
          title="Non-retryable"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Retry Queue Size"
      sources={storeSources}
      tenantSource={tenantSource}
      tooltip={`Total size of the retry queues across all processors in all LDR jobs`}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Bytes} label="bytes">
        {nodeIDs.map(node => (
          <Metric
            key={node}
            name="cr.node.logical_replication.retry_queue_bytes"
            title={nodeDisplayName(nodeDisplayNameByID, node)}
            sources={[node]}
          />
        ))}
      </Axis>
    </LineGraph>,
  ];
}
