import React from "react";
import _ from "lodash";

import { LineGraph } from "src/views/cluster/components/linegraph";
import { Metric, Axis, AxisUnits } from "src/views/shared/components/metricQuery";

import { GraphDashboardProps, nodeDisplayName, storeIDsForNode } from "./dashboardUtils";

export default function (props: GraphDashboardProps) {
  const { nodeIDs, nodesSummary, nodeSources, tooltipSelection } = props;

  return [
    <LineGraph title="Live Node Count" tooltip="The number of live nodes in the cluster.">
      <Axis label="nodes">
        <Metric name="cr.node.liveness.livenodes" title="Live Nodes" aggregateMax />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="CPU Time (per-node)"
      sources={nodeSources}
      tooltip={`CPU usage per node, in CPU seconds per second. E.g. if a node has four
        cores all at 100%, the value will be 4.`}
    >
      <Axis units={AxisUnits.Count} label="cpu seconds / second">
        {
          _.map(nodeIDs, (nid) => {
            return (
              <Metric
                name="cr.node.sys.cpu.user.percent"
                title={nodeDisplayName(nodesSummary, nid)}
                sources={[nid]}
              />
            );
          })
        }
      </Axis>
    </LineGraph>,

    <LineGraph
      title="CPU Time (aggregated)"
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

    <LineGraph
      title="Memory Usage (per-node RSS)"
      sources={nodeSources}
      tooltip={`Per-node Resident Set Size.`}
    >
      <Axis units={AxisUnits.Bytes} label="memory usage">
        {
          _.map(nodeIDs, (nid) => {
            return (
              <Metric
                name="cr.node.sys.rss"
                title={nodeDisplayName(nodesSummary, nid)}
                sources={[nid]}
              />
            );
          })
        }
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Memory Usage (aggregated)"
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

    <LineGraph
      title="Goroutine Count"
      sources={nodeSources}
      tooltip={
        `The number of Goroutines ${tooltipSelection}.
           This count should rise and fall based on load.`
      }
    >
      <Axis label="goroutines">
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
      <Axis label="runs">
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
      <Axis units={AxisUnits.Duration} label="pause time">
        <Metric name="cr.node.sys.gc.pause.ns" title="GC Pause Time" nonNegativeRate />
      </Axis>
    </LineGraph>,

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
  ];
}
