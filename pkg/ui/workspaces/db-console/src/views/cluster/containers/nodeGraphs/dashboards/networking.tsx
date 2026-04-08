// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { AxisUnits } from "@cockroachlabs/cluster-ui";
import React from "react";

import LineGraph from "src/views/cluster/components/linegraph";
import { Axis, Metric } from "src/views/shared/components/metricQuery";

import { GraphDashboardProps, nodeDisplayName } from "./dashboardUtils";

export default function (props: GraphDashboardProps) {
  const { nodeIDs, nodeSources, nodeDisplayNameByID, tenantSource } = props;

  return [
    <LineGraph title="Network Bytes Sent" showMetricsInTooltip={true}>
      <Axis units={AxisUnits.Bytes} label="bytes">
        <Metric
          name="cr.node.sys.host.net.send.bytes"
          sources={nodeSources}
          perSource
          sourceDisplayNames={nodeDisplayNameByID}
          tenantSource={tenantSource}
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph title="Network Bytes Received" showMetricsInTooltip={true}>
      <Axis units={AxisUnits.Bytes} label="bytes">
        <Metric
          name="cr.node.sys.host.net.recv.bytes"
          sources={nodeSources}
          perSource
          sourceDisplayNames={nodeDisplayNameByID}
          tenantSource={tenantSource}
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="RPC Heartbeat Latency: 50th percentile"
      isKvGraph={false}
      tooltip={`Round-trip latency for recent successful outgoing heartbeats.`}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        <Metric
          name="cr.node.round-trip-latency-p50"
          sources={nodeSources}
          perSource
          sourceDisplayNames={nodeDisplayNameByID}
          tenantSource={tenantSource}
          downsampleMax
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="RPC Heartbeat Latency: 99th percentile"
      isKvGraph={false}
      tooltip={`Round-trip latency for recent successful outgoing heartbeats.`}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        <Metric
          name="cr.node.round-trip-latency-p99"
          sources={nodeSources}
          perSource
          sourceDisplayNames={nodeDisplayNameByID}
          tenantSource={tenantSource}
          downsampleMax
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Unhealthy RPC Connections"
      tooltip={`The number of outgoing connections on each node that are in an
        unhealthy state.`}
      showMetricsInTooltip={true}
    >
      <Axis label="connections">
        <Metric
          name="cr.node.rpc.connection.unhealthy"
          sources={nodeSources}
          perSource
          sourceDisplayNames={nodeDisplayNameByID}
          tenantSource={tenantSource}
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Network Packet Errors and Drops"
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Count} label="packets">
        {nodeIDs.flatMap(nid => [
          <Metric
            key={`${nid}-recv-err`}
            name="cr.node.sys.host.net.recv.err"
            title={`${nodeDisplayName(nodeDisplayNameByID, nid)} - Recv Errors`}
            sources={[nid]}
            tenantSource={tenantSource}
            nonNegativeRate
          />,
          <Metric
            key={`${nid}-recv-drop`}
            name="cr.node.sys.host.net.recv.drop"
            title={`${nodeDisplayName(nodeDisplayNameByID, nid)} - Recv Drops`}
            sources={[nid]}
            tenantSource={tenantSource}
            nonNegativeRate
          />,
          <Metric
            key={`${nid}-send-err`}
            name="cr.node.sys.host.net.send.err"
            title={`${nodeDisplayName(nodeDisplayNameByID, nid)} - Send Errors`}
            sources={[nid]}
            tenantSource={tenantSource}
            nonNegativeRate
          />,
          <Metric
            key={`${nid}-send-drop`}
            name="cr.node.sys.host.net.send.drop"
            title={`${nodeDisplayName(nodeDisplayNameByID, nid)} - Send Drops`}
            sources={[nid]}
            tenantSource={tenantSource}
            nonNegativeRate
          />,
        ])}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="TCP Retransmits"
      tooltip={
        "The number of TCP segments retransmitted. Some retransmissions are benign, but phase changes can be indicative of network congestion or overloaded peers."
      }
      showMetricsInTooltip={true}
    >
      <Axis label="segments">
        <Metric
          name="cr.node.sys.host.net.send.tcp.retrans_segs"
          sources={nodeSources}
          perSource
          sourceDisplayNames={nodeDisplayNameByID}
          tenantSource={tenantSource}
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Proxy requests"
      tooltip={`The number of proxy attempts each gateway node is initiating.`}
      showMetricsInTooltip={true}
    >
      <Axis label="requests">
        <Metric
          name="cr.node.distsender.rpc.proxy.sent"
          sources={nodeSources}
          perSource
          sourceDisplayNames={nodeDisplayNameByID}
          tenantSource={tenantSource}
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Proxy request errors"
      tooltip={`The number of proxy attempts which resulted in an error.`}
      showMetricsInTooltip={true}
    >
      <Axis label="errors">
        <Metric
          name="cr.node.distsender.rpc.proxy.err"
          sources={nodeSources}
          perSource
          sourceDisplayNames={nodeDisplayNameByID}
          tenantSource={tenantSource}
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Proxy forwards"
      tooltip={`The number of proxy requests each server node is attempting to foward.`}
      showMetricsInTooltip={true}
    >
      <Axis label="requests">
        <Metric
          name="cr.node.distsender.rpc.proxy.forward.sent"
          sources={nodeSources}
          perSource
          sourceDisplayNames={nodeDisplayNameByID}
          tenantSource={tenantSource}
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Proxy forward errors"
      tooltip={`The number of proxy forward attempts which resulted in an error.`}
      showMetricsInTooltip={true}
    >
      <Axis label="errors">
        <Metric
          name="cr.node.distsender.rpc.proxy.forward.err"
          sources={nodeSources}
          perSource
          sourceDisplayNames={nodeDisplayNameByID}
          tenantSource={tenantSource}
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,
  ];
}
