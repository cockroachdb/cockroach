// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// THIS FILE IS GENERATED. DO NOT EDIT.
// To regenerate: ./dev generate dashboards

import { AxisUnits } from "@cockroachlabs/cluster-ui";
import React from "react";

import LineGraph from "src/views/cluster/components/linegraph";

import { Axis, Metric } from "src/views/shared/components/metricQuery";

import {
  GraphDashboardProps,
  nodeDisplayName,
  storeIDsForNode,
} from "./dashboardUtils";

export default function (props: GraphDashboardProps) {
  const {
    nodeIDs,
    nodeSources,
    storeSources,
    tooltipSelection,
    nodeDisplayNameByID,
    storeIDsByNodeID,
    tenantSource,
  } = props;

  return [
    <LineGraph
      title="Live Node Count"
      isKvGraph={false}
      
      tenantSource={tenantSource}
      tooltip={<div>The number of live nodes in the cluster.</div>}
      showMetricsInTooltip={true}
      preCalcGraphSize={false}
    >
      <Axis units={AxisUnits.Count} label="nodes">
        <Metric
          name="cr.node.liveness.livenodes"
          title="Live Nodes"
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Memory Usage"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={
        <div>
          Memory in use ${tooltipSelection}&nbsp;
          <em>
            <dl>
            <dt>RSS</dt>
            <dd>Total memory in use by CockroachDB</dd>
            <dt>Go Allocated</dt>
            <dd>Memory allocated by the Go layer</dd>
            <dt>Go Total</dt>
            <dd>Total memory managed by the Go layer</dd>
            <dt>Go Limit</dt>
            <dd>Go soft memory limit</dd>
            <dt>C Allocated</dt>
            <dd>Memory allocated by the C layer</dd>
            <dt>C Total</dt>
            <dd>Total memory managed by the C layer</dd>
          </dl>
          </em>
        </div>
      }
      showMetricsInTooltip={true}
      preCalcGraphSize={false}
    >
      <Axis units={AxisUnits.Bytes} label="memory usage">
        <Metric
          name="cr.node.sys.cgo.allocbytes"
          title="CGo Allocated"
        />
        <Metric
          name="cr.node.sys.cgo.totalbytes"
          title="CGo Total"
        />
        <Metric
          name="cr.node.sys.go.allocbytes"
          title="Go Allocated"
        />
        <Metric
          name="cr.node.sys.go.limitbytes"
          title="Go Limit"
        />
        <Metric
          name="cr.node.sys.go.totalbytes"
          title="Go Total"
        />
        <Metric
          name="cr.node.sys.rss"
          title="Total memory (RSS)"
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Goroutine Count"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={<div>The number of Goroutines ${tooltipSelection}</div>}
      showMetricsInTooltip={true}
      preCalcGraphSize={false}
    >
      <Axis units={AxisUnits.Count} label="goroutines">
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
            name="cr.node.sys.goroutines"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Goroutine Scheduling Latency: 99th percentile"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={<div>P99 scheduling latency for goroutines</div>}
      showMetricsInTooltip={true}
      preCalcGraphSize={false}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
            name="cr.node.go.scheduler_latency-p99"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
            downsampleMax
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Runnable Goroutines per CPU"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={<div>The number of Goroutines waiting for CPU ${tooltipSelection}</div>}
      showMetricsInTooltip={true}
      preCalcGraphSize={false}
    >
      <Axis units={AxisUnits.Count} label="goroutines">
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
            name="cr.node.sys.runnable.goroutines.per.cpu"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="GC Runs"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={<div>The number of times that Go's garbage collector was invoked per second ${tooltipSelection}</div>}
      showMetricsInTooltip={true}
      preCalcGraphSize={false}
    >
      <Axis units={AxisUnits.Count} label="runs">
        <Metric
          name="cr.node.sys.gc.count"
          title="GC Runs"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="GC Pause Time"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={<div>The amount of processor time used by Go's garbage collector per second ${tooltipSelection} During garbage collection, application code execution is paused.</div>}
      showMetricsInTooltip={true}
      preCalcGraphSize={false}
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
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={<div>The time it takes from deciding to stop-the-world (gc related) until all Ps are stopped ${tooltipSelection}</div>}
      showMetricsInTooltip={true}
      preCalcGraphSize={false}
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
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={<div>Estimated total CPU time user goroutines spent performing GC tasks on processors ${tooltipSelection}</div>}
      showMetricsInTooltip={true}
      preCalcGraphSize={false}
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
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={<div>The stop-the-world pause time during non-gc process ${tooltipSelection}</div>}
      showMetricsInTooltip={true}
      preCalcGraphSize={false}
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
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={<div>The time it takes from deciding to stop-the-world (non-gc-related) until all Ps are stopped ${tooltipSelection}</div>}
      showMetricsInTooltip={true}
      preCalcGraphSize={false}
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
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={<div>The amount of CPU time used by CockroachDB (User) and system-level operations (Sys) ${tooltipSelection}</div>}
      showMetricsInTooltip={true}
      preCalcGraphSize={false}
    >
      <Axis units={AxisUnits.Duration} label="cpu time">
        <Metric
          name="cr.node.sys.cpu.sys.ns"
          title="Sys CPU Time"
          nonNegativeRate
        />
        <Metric
          name="cr.node.sys.cpu.user.ns"
          title="User CPU Time"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Clock Offset"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={<div>Mean clock offset of each node against the rest of the cluster.</div>}
      showMetricsInTooltip={true}
      preCalcGraphSize={false}
    >
      <Axis units={AxisUnits.Duration} label="offset">
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
            name="cr.node.clock-offset.meannanos"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
          />
        ))}
      </Axis>
    </LineGraph>
  ];
}