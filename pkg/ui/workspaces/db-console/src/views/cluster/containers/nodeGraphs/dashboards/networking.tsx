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
      title="Network Bytes Sent"
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
            name="cr.node.sys.host.net.send.bytes"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Network Bytes Received"
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
            name="cr.node.sys.host.net.recv.bytes"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="RPC Heartbeat Latency: 50th percentile"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={<div>Round-trip latency for recent successful outgoing heartbeats.</div>}
      showMetricsInTooltip={true}
      preCalcGraphSize={false}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
            name="cr.node.round-trip-latency-p50"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
            downsampleMax
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="RPC Heartbeat Latency: 99th percentile"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={<div>Round-trip latency for recent successful outgoing heartbeats.</div>}
      showMetricsInTooltip={true}
      preCalcGraphSize={false}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
            name="cr.node.round-trip-latency-p99"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
            downsampleMax
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Unhealthy RPC Connections"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={<div>Number of connections in an unhealthy state</div>}
      showMetricsInTooltip={true}
      preCalcGraphSize={false}
    >
      <Axis units={AxisUnits.Count} label="connections">
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
            name="cr.node.rpc.connection.unhealthy"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Network Packet Errors and Drops"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      
      showMetricsInTooltip={true}
      preCalcGraphSize={false}
    >
      <Axis units={AxisUnits.Count} label="packets">
        {nodeIDs.map(nid => (
          <Metric
            key={`${nid}-recv-drop`}
            name="cr.node.sys.host.net.recv.drop"
            title={`${nodeDisplayName(nodeDisplayNameByID, nid)}-Recv Drops`}
            sources={[nid]}
            nonNegativeRate
          />
        ))}
        {nodeIDs.map(nid => (
          <Metric
            key={`${nid}-recv-err`}
            name="cr.node.sys.host.net.recv.err"
            title={`${nodeDisplayName(nodeDisplayNameByID, nid)}-Recv Errors`}
            sources={[nid]}
            nonNegativeRate
          />
        ))}
        {nodeIDs.map(nid => (
          <Metric
            key={`${nid}-send-drop`}
            name="cr.node.sys.host.net.send.drop"
            title={`${nodeDisplayName(nodeDisplayNameByID, nid)}-Send Drops`}
            sources={[nid]}
            nonNegativeRate
          />
        ))}
        {nodeIDs.map(nid => (
          <Metric
            key={`${nid}-send-err`}
            name="cr.node.sys.host.net.send.err"
            title={`${nodeDisplayName(nodeDisplayNameByID, nid)}-Send Errors`}
            sources={[nid]}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="TCP Retransmits"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={<div>The number of TCP segments retransmitted. Some retransmissions are benign, but phase changes can be indicative of network congestion or overloaded peers.</div>}
      showMetricsInTooltip={true}
      preCalcGraphSize={false}
    >
      <Axis units={AxisUnits.Count} label="segments">
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
            name="cr.node.sys.host.net.send.tcp.retrans_segs"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Proxy requests"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={<div>Number of proxy request attempts</div>}
      showMetricsInTooltip={true}
      preCalcGraphSize={false}
    >
      <Axis units={AxisUnits.Count} label="requests">
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
            name="cr.node.distsender.rpc.proxy.sent"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Proxy request errors"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={<div>Number of proxy request failures</div>}
      showMetricsInTooltip={true}
      preCalcGraphSize={false}
    >
      <Axis units={AxisUnits.Count} label="errors">
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
            name="cr.node.distsender.rpc.proxy.err"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Proxy forwards"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={<div>Number of proxy forward attempts</div>}
      showMetricsInTooltip={true}
      preCalcGraphSize={false}
    >
      <Axis units={AxisUnits.Count} label="forwards">
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
            name="cr.node.distsender.rpc.proxy.forward.sent"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Proxy forward errors"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={<div>Number of proxy forward failures</div>}
      showMetricsInTooltip={true}
      preCalcGraphSize={false}
    >
      <Axis units={AxisUnits.Count} label="errors">
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
            name="cr.node.distsender.rpc.proxy.forward.err"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>
  ];
}