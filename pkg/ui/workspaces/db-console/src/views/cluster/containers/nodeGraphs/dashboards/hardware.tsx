// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";

import { LineGraph } from "src/views/cluster/components/linegraph";
import {
  Metric,
  Axis,
  AxisUnits,
} from "src/views/shared/components/metricQuery";

import {
  GraphDashboardProps,
  nodeDisplayName,
  storeIDsForNode,
} from "./dashboardUtils";
import { AvailableDiscCapacityGraphTooltip } from "src/views/cluster/containers/nodeGraphs/dashboards/graphTooltips";

// TODO(vilterp): tooltips

export default function (props: GraphDashboardProps) {
  const {
    nodeIDs,
    nodesSummary,
    nodeSources,
    storeSources,
    tooltipSelection,
  } = props;

  return [
    <LineGraph title="CPU Percent" sources={nodeSources}>
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

    <LineGraph
      title="Memory Usage"
      sources={nodeSources}
      tooltip={<div>Memory in use {tooltipSelection}</div>}
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

    <LineGraph title="Disk Read MiB/s" sources={nodeSources}>
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

    <LineGraph title="Disk Write MiB/s" sources={nodeSources}>
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

    <LineGraph title="Disk Read IOPS" sources={nodeSources}>
      <Axis units={AxisUnits.Count} label="IOPS">
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

    <LineGraph title="Disk Write IOPS" sources={nodeSources}>
      <Axis units={AxisUnits.Count} label="IOPS">
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

    <LineGraph title="Disk Ops In Progress" sources={nodeSources}>
      <Axis units={AxisUnits.Count} label="Ops">
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
      title="Available Disk Capacity"
      sources={storeSources}
      tooltip={<AvailableDiscCapacityGraphTooltip />}
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

    <LineGraph title="Network Bytes Received" sources={nodeSources}>
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

    <LineGraph title="Network Bytes Sent" sources={nodeSources}>
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
