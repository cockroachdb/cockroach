import React from "react";
import _ from "lodash";

import { AggregationLevel } from "src/redux/aggregationLevel";
import { LineGraph } from "src/views/cluster/components/linegraph";
import { Metric, Axis, AxisUnits } from "src/views/shared/components/metricQuery";

import { GraphDashboardProps, nodeDisplayName, storeIDsForNode } from "./dashboardUtils";

export default function (props: GraphDashboardProps) {
  const { nodeIDs, nodesSummary, nodeSources, tooltipSelection, aggregationLevel } = props;

  function default_aggregate(name: string, title: string, otherProps?: { [key: string]: boolean }) {
    if (aggregationLevel === AggregationLevel.Cluster) {
      return (
        <Metric name={name} title={title} sources={nodeSources} {...otherProps} />
      );
    }

    return nodeIDs.map((nid) => (
      <Metric name={name} title={nodeDisplayName(nodesSummary, nid)} sources={[nid]} {...otherProps} />
    ));
  }

  const charts = [];

  charts.push(
    <LineGraph title="Live Node Count" tooltip="The number of live nodes in the cluster.">
      <Axis label="nodes">
        {default_aggregate("cr.node.liveness.livenodes", "Live Nodes", { aggregateMax: true })}
      </Axis>
    </LineGraph>,
  );

  charts.push(
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
      <Axis units={AxisUnits.Bytes} label="memory usage">
        <Metric name="cr.node.sys.rss" title="Total memory (RSS)" />
        <Metric name="cr.node.sys.go.allocbytes" title="Go Allocated" />
        <Metric name="cr.node.sys.go.totalbytes" title="Go Total" />
        <Metric name="cr.node.sys.cgo.allocbytes" title="CGo Allocated" />
        <Metric name="cr.node.sys.cgo.totalbytes" title="CGo Total" />
      </Axis>
    </LineGraph>,
  );

  charts.push(
    <LineGraph
      title="Goroutine Count"
      sources={nodeSources}
      tooltip={
        `The number of Goroutines ${tooltipSelection}.
           This count should rise and fall based on load.`
      }
    >
      <Axis label="goroutines">
        {default_aggregate("cr.node.sys.goroutines", "Goroutine Count")}
      </Axis>
    </LineGraph>,
  );

  // TODO(mrtracy): The following two graphs are a good first example of a graph with
  // two axes; the two series should be highly correlated, but have different units.

  charts.push(
    <LineGraph
      title="GC Runs"
      sources={nodeSources}
      tooltip={
        `The number of times that Go’s garbage collector was invoked per second ${tooltipSelection}.`
      }
    >
      <Axis label="runs">
        {default_aggregate("cr.node.sys.gc.count", "GC Runs", { nonNegativeRate: true })}
      </Axis>
    </LineGraph>,
  );

  charts.push(
    <LineGraph
      title="GC Pause Time"
      sources={nodeSources}
      tooltip={
        `The amount of processor time used by Go’s garbage collector
           per second ${tooltipSelection}.
           During garbage collection, application code execution is paused.`
      }
    >
      <Axis units={AxisUnits.Duration} label="pause time">
        {default_aggregate("cr.node.sys.gc.pause.ns", "GC Pause Time", { nonNegativeRate: true })}
      </Axis>
    </LineGraph>,
  );

  charts.push(
    <LineGraph
      title="CPU Time"
      sources={nodeSources}
      tooltip={
        `The amount of CPU time used by CockroachDB (User)
           and system-level operations (Sys) ${tooltipSelection}.`
      }
    >
      <Axis units={AxisUnits.Duration} label="cpu time">
        <Metric name="cr.node.sys.cpu.user.ns" title="User CPU Time" nonNegativeRate />
        <Metric name="cr.node.sys.cpu.sys.ns" title="Sys CPU Time" nonNegativeRate />
      </Axis>
    </LineGraph>,
  );

  charts.push(
    <LineGraph
      title="Clock Offset"
      sources={nodeSources}
      tooltip={`Mean clock offset of each node against the rest of the cluster.`}
    >
      <Axis label="offset" units={AxisUnits.Duration}>
        {
          _.map(nodeIDs, (nid) => (
            <Metric
              key={nid}
              name="cr.node.clock-offset.meannanos"
              title={nodeDisplayName(nodesSummary, nid)}
              sources={storeIDsForNode(nodesSummary, nid)}
            />
          ))
        }
      </Axis>
    </LineGraph>,
  );

  return charts;
}
