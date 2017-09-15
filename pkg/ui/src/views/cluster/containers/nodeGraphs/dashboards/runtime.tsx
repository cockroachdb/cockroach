import React from "react";

import { LineGraph } from "src/views/cluster/components/linegraph";
import { Metric, Axis, AxisUnits } from "src/views/shared/components/metricQuery";

import { GraphDashboardProps } from "./dashboardUtils";

export default function (props: GraphDashboardProps) {
  const { nodeSources, tooltipSelection } = props;

  return [
    <LineGraph title="Live Node Count" tooltip="The number of live nodes in the cluster.">
      <Axis>
        <Metric name="cr.node.liveness.livenodes" title="Live Nodes" aggregateMax />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Memory Usage"
      sources={nodeSources}
      tooltip={(
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
      )}
    >
      <Axis units={AxisUnits.Bytes}>
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
      tooltip={
        `The number of Goroutines ${tooltipSelection}.
           This count should rise and fall based on load.`
      }
    >
      <Axis>
        <Metric name="cr.node.sys.goroutines" title="Goroutine Count" />
      </Axis>
    </LineGraph>,

    // TODO(mrtracy): The following two graphs are a good first example of a graph with
    // two axes; the two series should be highly correlated, but have different units.
    <LineGraph
      title="GC Runs"
      sources={nodeSources}
      tooltip={
        `The number of times that Go’s garbage collector was invoked per second ${tooltipSelection}.`
      }
    >
      <Axis>
        <Metric name="cr.node.sys.gc.count" title="GC Runs" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="GC Pause Time"
      sources={nodeSources}
      tooltip={
        `The amount of processor time used by Go’s garbage collector
           per second ${tooltipSelection}.
           During garbage collection, application code execution is paused.`
      }
    >
      <Axis units={AxisUnits.Duration}>
        <Metric name="cr.node.sys.gc.pause.ns" title="GC Pause Time" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="CPU Time"
      sources={nodeSources}
      tooltip={
        `The amount of CPU time used by CockroachDB (User)
           and system-level operations (Sys) ${tooltipSelection}.`
      }
    >
      <Axis units={AxisUnits.Duration}>
        <Metric name="cr.node.sys.cpu.user.ns" title="User CPU Time" nonNegativeRate />
        <Metric name="cr.node.sys.cpu.sys.ns" title="Sys CPU Time" nonNegativeRate />
        <Metric name="cr.node.sys.gc.pause.ns" title="GC Pause Time" nonNegativeRate />
      </Axis>
    </LineGraph>,
  ];
}
