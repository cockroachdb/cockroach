// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// THIS FILE IS GENERATED. DO NOT EDIT.
// To regenerate: ./dev generate dashboards

import { AxisUnits } from "@cockroachlabs/cluster-ui";
import React from "react";

import LineGraph from "src/views/cluster/components/linegraph";
import { AvailableDiscCapacityGraphTooltip } from "src/views/cluster/containers/nodeGraphs/dashboards/graphTooltips";
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
      title="CPU Percent"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={<div>CPU usage for the CRDB nodes ${tooltipSelection}</div>}
      showMetricsInTooltip={true}
      preCalcGraphSize={false}
    >
      <Axis units={AxisUnits.Percentage} label="CPU">
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
            name="cr.node.sys.cpu.combined.percent-normalized"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Host CPU Percent"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={<div>Machine-wide CPU usage ${tooltipSelection}</div>}
      showMetricsInTooltip={true}
      preCalcGraphSize={false}
    >
      <Axis units={AxisUnits.Percentage} label="CPU">
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
            name="cr.node.sys.cpu.host.combined.percent-normalized"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
          />
        ))}
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
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
            name="cr.node.sys.rss"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Disk Read Bytes/s"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      
      showMetricsInTooltip={true}
      preCalcGraphSize={false}
    >
      <Axis units={AxisUnits.Bytes} label="bytes">
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
            name="cr.node.sys.host.disk.read.bytes"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Disk Write Bytes/s"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      
      showMetricsInTooltip={true}
      preCalcGraphSize={false}
    >
      <Axis units={AxisUnits.Bytes} label="bytes">
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
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
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      
      showMetricsInTooltip={true}
      preCalcGraphSize={false}
    >
      <Axis units={AxisUnits.Count} label="IOPS">
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
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
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      
      showMetricsInTooltip={true}
      preCalcGraphSize={false}
    >
      <Axis units={AxisUnits.Count} label="IOPS">
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
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
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      
      showMetricsInTooltip={true}
      preCalcGraphSize={false}
    >
      <Axis units={AxisUnits.Count} label="Ops">
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
            name="cr.node.sys.host.disk.iopsinprogress"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Available Disk Capacity"
      isKvGraph={false}
      sources={storeSources}
      tenantSource={tenantSource}
      tooltip={<AvailableDiscCapacityGraphTooltip />}
      showMetricsInTooltip={true}
      preCalcGraphSize={false}
    >
      <Axis units={AxisUnits.Bytes} label="capacity">
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
            name="cr.store.capacity.available"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={storeIDsForNode(storeIDsByNodeID, nid)}
          />
        ))}
      </Axis>
    </LineGraph>
  ];
}