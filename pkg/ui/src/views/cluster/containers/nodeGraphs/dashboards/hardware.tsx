import React from "react";

import { AggregationLevel } from "src/redux/aggregationLevel";
import { LineGraph } from "src/views/cluster/components/linegraph";
import { Metric, Axis, AxisUnits } from "src/views/shared/components/metricQuery";

import { GraphDashboardProps, nodeDisplayName, storeIDsForNode } from "./dashboardUtils";

// TODO(vilterp): tooltips

export default function (props: GraphDashboardProps) {
  const { nodeIDs, nodesSummary, nodeSources, storeSources, tooltipSelection } = props;

  const charts = [];

  // We can't really aggregate CPU percent at the moment.
  charts.push(
    <LineGraph
      title="CPU Percent"
      sources={nodeSources}
    >
      <Axis units={AxisUnits.Percentage} label="CPU">
        {nodeIDs.map((nid) => (
          <Metric
            name="cr.node.sys.cpu.combined.percent-normalized"
            title={nodeDisplayName(nodesSummary, nid)}
            sources={[nid]}
          />
        ))}
      </Axis>
    </LineGraph>,
  );

  if (props.aggregationLevel === AggregationLevel.Cluster) {
    charts.push(
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
          <Metric name="cr.node.sys.rss" title="Resident set size" />
        </Axis>
      </LineGraph>,
    );
  } else {
    charts.push(
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
    );
  }

  if (props.aggregationLevel === AggregationLevel.Cluster) {
    charts.push(
      <LineGraph
        title="Disk Throughput"
        sources={nodeSources}
      >
        <Axis units={AxisUnits.Bytes} label="throughput">
          <Metric
            name="cr.node.sys.host.disk.read.bytes"
            title="Bytes Read" />
          <Metric
            name="cr.node.sys.host.disk.write.bytes"
            title="Bytes Written" />
        </Axis>
      </LineGraph>,
    );
  } else {
    charts.push(
      <LineGraph
        title="Disk Throughput"
        subtitle="Reads"
        sources={nodeSources}
      >
        <Axis units={AxisUnits.Bytes} label="throughput">
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
    );
    charts.push(
      <LineGraph
        title="Disk Throughput"
        subtitle="Writes"
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
    );
  }

  if (props.aggregationLevel === AggregationLevel.Cluster) {
    charts.push(
      <LineGraph
        title="Disk Ops"
        sources={nodeSources}
      >
        <Axis units={AxisUnits.Count} label="Read Ops">
          <Metric
            name="cr.node.sys.host.disk.read.count"
            title="Disk Read Operations" />
          <Metric
            name="cr.node.sys.host.disk.write.count"
            title="Disk Write Operations" />
        </Axis>
      </LineGraph>,
    );
  } else {
    charts.push(
      <LineGraph
        title="Disk Ops"
        subtitle="Reads"
        sources={nodeSources}
      >
        <Axis units={AxisUnits.Count} label="Read Ops">
          {nodeIDs.map((nid) => (
            <Metric
              name="cr.node.sys.host.disk.read.count"
              title={nodeDisplayName(nodesSummary, nid)}
              sources={[nid]}
              nonNegativeRate
            />
          ))}
        </Axis>
      </LineGraph>,
    );
    charts.push(
      <LineGraph
        title="Disk Ops"
        subtitle="Writes"
        sources={nodeSources}
      >
        <Axis units={AxisUnits.Count} label="Write Ops">
          {nodeIDs.map((nid) => (
            <Metric
              name="cr.node.sys.host.disk.write.count"
              title={nodeDisplayName(nodesSummary, nid)}
              sources={[nid]}
              nonNegativeRate
            />
          ))}
        </Axis>
      </LineGraph>,
    );
  }

  if (props.aggregationLevel === AggregationLevel.Cluster) {
    charts.push(
      <LineGraph
        title="Disk IOPS In Progress"
        sources={nodeSources}
      >
        <Axis units={AxisUnits.Count} label="IOPS">
          <Metric
            name="cr.node.sys.host.disk.iopsinprogress"
            title="IOPS in Progress" />
        </Axis>
      </LineGraph>,
    );
  } else {
    charts.push(
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
    );
  }

  if (props.aggregationLevel === AggregationLevel.Cluster) {
    charts.push(
      <LineGraph
        title="Available Disk Capacity"
        sources={storeSources}
      >
        <Axis units={AxisUnits.Bytes} label="capacity">
          <Metric
            name="cr.store.capacity.available"
            title="Available" />
          <Metric
            name="cr.store.capacity.used"
            title="Used" />
        </Axis>
      </LineGraph>,
    );
  } else {
    charts.push(
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
    );
  }

  if (props.aggregationLevel === AggregationLevel.Cluster) {
    charts.push(
      <LineGraph
        title="Network Throughput"
        sources={nodeSources}
      >
        <Axis units={AxisUnits.Bytes} label="bytes">
          <Metric
            name="cr.node.sys.host.net.recv.bytes"
            title="Bytes Received" />
          <Metric
            name="cr.node.sys.host.net.send.bytes"
            title="Bytes Sent" />
        </Axis>
      </LineGraph>,
    );
  } else {
    charts.push(
      <LineGraph
        title="Network Throughput"
        subtitle="Received"
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
    );
    charts.push(
      <LineGraph
        title="Network Throughput"
        subtitle="Sent"
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
    );
  }

  return charts;
}
