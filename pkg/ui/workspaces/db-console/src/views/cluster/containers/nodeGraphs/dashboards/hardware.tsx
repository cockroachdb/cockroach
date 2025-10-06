// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { AxisUnits } from "@cockroachlabs/cluster-ui";
import React from "react";

import LineGraph from "src/views/cluster/components/linegraph";
import { AvailableDiscCapacityGraphTooltip } from "src/views/cluster/containers/nodeGraphs/dashboards/graphTooltips";
import { Metric, Axis } from "src/views/shared/components/metricQuery";

import {
  GraphDashboardProps,
  nodeDisplayName,
  storeIDsForNode,
} from "./dashboardUtils";

// TODO(vilterp): tooltips

export default function (props: GraphDashboardProps) {
  const {
    nodeIDs,
    nodeDisplayNameByID,
    storeIDsByNodeID,
    nodeSources,
    storeSources,
    tooltipSelection,
    tenantSource,
  } = props;

  return [
    <LineGraph
      title="CPU Percent"
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={<div>CPU usage for the CRDB nodes {tooltipSelection}</div>}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Percentage} label="CPU">
        {nodeIDs.map(nid => (
          <Metric
            name="cr.node.sys.cpu.combined.percent-normalized"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Host CPU Percent"
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={<div>Machine-wide CPU usage {tooltipSelection}</div>}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Percentage} label="CPU">
        {nodeIDs.map(nid => (
          <Metric
            name="cr.node.sys.cpu.host.combined.percent-normalized"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Memory Usage"
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={<div>Memory in use {tooltipSelection}</div>}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Bytes} label="memory usage">
        {nodeIDs.map(nid => (
          <Metric
            name="cr.node.sys.rss"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Disk Read MiB/s"
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Bytes} label="bytes">
        {nodeIDs.map(nid => (
          <Metric
            name="cr.node.sys.host.disk.read.bytes"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Disk Write MiB/s"
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Bytes} label="bytes">
        {nodeIDs.map(nid => (
          <Metric
            name="cr.node.sys.host.disk.write.bytes"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Disk Read IOPS"
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Count} label="IOPS">
        {nodeIDs.map(nid => (
          <Metric
            name="cr.node.sys.host.disk.read.count"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Disk Write IOPS"
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Count} label="IOPS">
        {nodeIDs.map(nid => (
          <Metric
            name="cr.node.sys.host.disk.write.count"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Disk Ops In Progress"
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Count} label="Ops">
        {nodeIDs.map(nid => (
          <Metric
            name="cr.node.sys.host.disk.iopsinprogress"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Available Disk Capacity"
      sources={storeSources}
      tenantSource={tenantSource}
      tooltip={<AvailableDiscCapacityGraphTooltip />}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Bytes} label="capacity">
        {nodeIDs.map(nid => (
          <Metric
            name="cr.store.capacity.available"
            sources={storeIDsForNode(storeIDsByNodeID, nid)}
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
          />
        ))}
      </Axis>
    </LineGraph>,
  ];
}
