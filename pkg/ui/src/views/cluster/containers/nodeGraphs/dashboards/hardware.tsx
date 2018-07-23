import React from "react";

import { LineGraph } from "src/views/cluster/components/linegraph";
import { Metric, Axis, AxisUnits } from "src/views/shared/components/metricQuery";

import { GraphDashboardProps, nodeDisplayName } from "./dashboardUtils";
import * as docsURL from "oss/src/util/docs";

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
      <Axis units={AxisUnits.Bytes} label="memory usage">
        {nodeIDs.map((nid) => (
          <Metric
            name="cr.node.sys.rss"
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
      title="Disk Capacity"
      sources={storeSources}
      tooltip={(
        <div>
          <dl>
            <dt>Capacity</dt>
            <dd>
              Total disk space available {tooltipSelection} to CockroachDB.
              {" "}
              <em>
                Control this value per node with the
                {" "}
                <code>
                  <a href={docsURL.startFlags} target="_blank">
                    --store
                  </a>
                </code>
                {" "}
                flag.
              </em>
            </dd>
            <dt>Available</dt>
            <dd>Free disk space available {tooltipSelection} to CockroachDB.</dd>
            <dt>Used</dt>
            <dd>Disk space used {tooltipSelection} by CockroachDB.</dd>
          </dl>
        </div>
      )}
    >
      <Axis units={AxisUnits.Bytes} label="capacity">
        <Metric name="cr.store.capacity" title="Capacity" />
        <Metric name="cr.store.capacity.available" title="Available" />
        <Metric name="cr.store.capacity.used" title="Used" />
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
