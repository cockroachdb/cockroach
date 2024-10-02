// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { AxisUnits } from "@cockroachlabs/cluster-ui";
import map from "lodash/map";
import React from "react";

import LineGraph from "src/views/cluster/components/linegraph";
import { Metric, Axis } from "src/views/shared/components/metricQuery";

import { GraphDashboardProps, nodeDisplayName } from "./dashboardUtils";

export default function (props: GraphDashboardProps) {
  const {
    nodeIDs,
    nodeSources,
    tooltipSelection,
    nodeDisplayNameByID,
    tenantSource,
  } = props;

  return [
    <LineGraph
      title="Live Node Count"
      tenantSource={tenantSource}
      tooltip={`The number of live nodes in the cluster.`}
      showMetricsInTooltip={true}
    >
      <Axis label="nodes">
        <Metric
          name="cr.node.liveness.livenodes"
          title="Live Nodes"
          aggregateMax
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Memory Usage"
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={
        <div>
          {`Memory in use ${tooltipSelection}:`}
          <dl>
            <dt>RSS</dt>
            <dd>Total memory in use by CockroachDB</dd>
            <dt>Go Allocated</dt>
            <dd>Memory allocated by the Go layer</dd>
            <dt>Go Total</dt>
            <dd>Total memory managed by the Go layer</dd>
            <dt>C Allocated</dt>
            <dd>Memory allocated by the C layer</dd>
            <dt>C Total</dt>
            <dd>Total memory managed by the C layer</dd>
          </dl>
        </div>
      }
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Bytes} label="memory usage">
        <Metric name="cr.node.sys.rss" title="Total memory (RSS)" />
        <Metric name="cr.node.sys.go.allocbytes" title="Go Allocated" />
        <Metric name="cr.node.sys.go.totalbytes" title="Go Total" />
        <Metric name="cr.node.sys.cgo.allocbytes" title="CGo Allocated" />
        <Metric name="cr.node.sys.cgo.totalbytes" title="CGo Total" />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Goroutine Count"
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`The number of Goroutines ${tooltipSelection}. This count should rise
          and fall based on load.`}
      showMetricsInTooltip={true}
    >
      <Axis label="goroutines">
        <Metric name="cr.node.sys.goroutines" title="Goroutine Count" />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Goroutine Scheduling Latency: 99th percentile"
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`P99 scheduling latency for goroutines`}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {nodeIDs.map(nid => (
          <>
            <Metric
              key={nid}
              name="cr.node.go.scheduler_latency-p99"
              title={nodeDisplayName(nodeDisplayNameByID, nid)}
              sources={[nid]}
              downsampleMax
            />
          </>
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Runnable Goroutines per CPU"
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`The number of Goroutines waiting for CPU ${tooltipSelection}. This
          count should rise and fall based on load.`}
      showMetricsInTooltip={true}
    >
      <Axis label="goroutines">
        {nodeIDs.map(nid => (
          <Metric
            name="cr.node.sys.runnable.goroutines.per.cpu"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
          />
        ))}
      </Axis>
    </LineGraph>,

    // TODO(mrtracy): The following two graphs are a good first example of a graph with
    // two axes; the two series should be highly correlated, but have different units.
    <LineGraph
      title="GC Runs"
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`The number of times that Go’s garbage collector was invoked per second ${tooltipSelection}.`}
      showMetricsInTooltip={true}
    >
      <Axis label="runs">
        <Metric name="cr.node.sys.gc.count" title="GC Runs" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="GC Pause Time"
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`The amount of processor time used by Go’s garbage collector per second
          ${tooltipSelection}. During garbage collection, application code
          execution is paused.`}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Duration} label="pause time">
        <Metric
          name="cr.node.sys.gc.pause.ns"
          title="GC Pause Time"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="GC Stopping Time"
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`The time it takes from deciding to
      stop-the-world (gc related) until all Ps are stopped
        ${tooltipSelection}.`}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Duration} label="pause time">
        <Metric
          name="cr.node.sys.gc.stop.ns"
          title="GC Stopping Time"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="GC Assist Time"
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`Estimated total CPU time user goroutines spent performing GC tasks on processors
        ${tooltipSelection}.`}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Duration} label="gc assist time">
        <Metric
          name="cr.node.sys.gc.assist.ns"
          title="GC Assist Time"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Non-GC Pause Time"
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`The stop-the-world pause time during
      non-gc process ${tooltipSelection}.`}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Duration} label="pause time">
        <Metric
          name="cr.node.sys.go.pause.other.ns"
          title="Non-GC Pause Time"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Non-GC Stopping Time"
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`The time it takes from deciding to stop-the-world 
      (non-gc-related) until all Ps are stopped
      ${tooltipSelection}.`}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Duration} label="pause time">
        <Metric
          name="cr.node.sys.go.stop.other.ns"
          title="Non-GC Stopping Time"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="CPU Time"
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`The amount of CPU time used by CockroachDB (User) and system-level
          operations (Sys) ${tooltipSelection}.`}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Duration} label="cpu time">
        <Metric
          name="cr.node.sys.cpu.user.ns"
          title="User CPU Time"
          nonNegativeRate
        />
        <Metric
          name="cr.node.sys.cpu.sys.ns"
          title="Sys CPU Time"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Clock Offset"
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`Mean clock offset of each node against the rest of the cluster.`}
      showMetricsInTooltip={true}
    >
      <Axis label="offset" units={AxisUnits.Duration}>
        {map(nodeIDs, nid => (
          <Metric
            key={nid}
            name="cr.node.clock-offset.meannanos"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
          />
        ))}
      </Axis>
    </LineGraph>,
  ];
}
