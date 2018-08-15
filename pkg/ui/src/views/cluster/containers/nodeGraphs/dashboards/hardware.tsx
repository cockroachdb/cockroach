import React from "react";

import { LineGraph } from "src/views/cluster/components/linegraph";
import { Metric, Axis, AxisUnits } from "src/views/shared/components/metricQuery";

import {GraphDashboardProps, nodeDisplayName, storeIDsForNode} from "./dashboardUtils";

// TODO(vilterp): tooltips

export default function (props: GraphDashboardProps) {
  const { nodeIDs, nodesSummary, nodeSources, storeSources, tooltipSelection } = props;

  return [
    <LineGraph
      title="User CPU Percent"
      sources={nodeSources}
    >
      <Axis units={AxisUnits.Percentage} label="CPU">
        {nodeIDs.map((nid) => (
          <Metric
            name="cr.node.sys.cpu.user.percent"
            title={nodeDisplayName(nodesSummary, nid)}
            sources={[nid]}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="System CPU Percent"
      sources={nodeSources}
    >
      <Axis units={AxisUnits.Percentage} label="CPU">
        {nodeIDs.map((nid) => (
          <Metric
            name="cr.node.sys.cpu.sys.percent"
            title={nodeDisplayName(nodesSummary, nid)}
            sources={[nid]}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Memory Usage"
      sources={nodeSources}
      tooltip={(
        <div>
          Memory in use {tooltipSelection}
        </div>
      )}
    >
      <Axis units={AxisUnits.Percentage} label="memory usage">
        {nodeIDs.map((nid) => (
          <Metric
            name="cr.node.sys.rss.percent"
            title={nodeDisplayName(nodesSummary, nid)}
            sources={[nid]}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Disk Read Bytes"
      sources={nodeSources}
    >
      <Axis units={AxisUnits.Bytes} label="bytes">
        {nodeIDs.map((nid) => (
          <Metric
            name="cr.node.sys.host.disk.read.bytes"
            title={nodeDisplayName(nodesSummary, nid)}
            sources={[nid]}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Disk Write Bytes"
      sources={nodeSources}
    >
      <Axis units={AxisUnits.Bytes} label="bytes">
        {nodeIDs.map((nid) => (
          <Metric
            name="cr.node.sys.host.disk.write.bytes"
            title={nodeDisplayName(nodesSummary, nid)}
            sources={[nid]}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Disk IOPS In Progress"
      sources={nodeSources}
    >
      <Axis units={AxisUnits.Count} label="IOPS">
        {nodeIDs.map((nid) => (
          <Metric
            name="cr.node.sys.host.disk.iopsinprogress"
            title={nodeDisplayName(nodesSummary, nid)}
            sources={[nid]}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Disk Read Time"
      sources={nodeSources}
    >
      <Axis units={AxisUnits.Duration} label="Read Time">
        {nodeIDs.map((nid) => (
          <Metric
            name="cr.node.sys.host.disk.read.time"
            title={nodeDisplayName(nodesSummary, nid)}
            sources={[nid]}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Disk Write Time"
      sources={nodeSources}
    >
      <Axis units={AxisUnits.Duration} label="Write Time">
        {nodeIDs.map((nid) => (
          <Metric
            name="cr.node.sys.host.disk.write.time"
            title={nodeDisplayName(nodesSummary, nid)}
            sources={[nid]}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Available Disk Capacity"
      sources={storeSources}
    >
      <Axis units={AxisUnits.Bytes} label="capacity">
        {nodeIDs.map((nid) => (
          <Metric
            name="cr.store.capacity.available"
            sources={storeIDsForNode(nodesSummary, nid)}
            title={nodeDisplayName(nodesSummary, nid)}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Network Bytes Received"
      sources={nodeSources}
    >
      <Axis units={AxisUnits.Bytes} label="bytes">
        {nodeIDs.map((nid) => (
          <Metric
            name="cr.node.sys.host.net.recv.bytes"
            title={nodeDisplayName(nodesSummary, nid)}
            sources={[nid]}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Network Bytes Sent"
      sources={nodeSources}
    >
      <Axis units={AxisUnits.Bytes} label="bytes">
        {nodeIDs.map((nid) => (
          <Metric
            name="cr.node.sys.host.net.send.bytes"
            title={nodeDisplayName(nodesSummary, nid)}
            sources={[nid]}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,
  ];
}
