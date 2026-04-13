// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { AxisUnits } from "@cockroachlabs/cluster-ui";
import React from "react";

import LineGraph from "src/views/cluster/components/linegraph";
import { Axis, Metric } from "src/views/shared/components/metricQuery";

import { GraphDashboardProps } from "./dashboardUtils";

export default function (props: GraphDashboardProps) {
  const { nodeSources, nodeDisplayNameByID, tenantSource } = props;

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
        <Metric
          name="cr.node.sys.host.net.recv.err"
          title="Recv Errors"
          sources={nodeSources}
          tenantSource={tenantSource}
          perSource
          sourceDisplayNames={nodeDisplayNameByID}
          nonNegativeRate
        />
        <Metric
          name="cr.node.sys.host.net.recv.drop"
          title="Recv Drops"
          sources={nodeSources}
          tenantSource={tenantSource}
          perSource
          sourceDisplayNames={nodeDisplayNameByID}
          nonNegativeRate
        />
        <Metric
          name="cr.node.sys.host.net.send.err"
          title="Send Errors"
          sources={nodeSources}
          tenantSource={tenantSource}
          perSource
          sourceDisplayNames={nodeDisplayNameByID}
          nonNegativeRate
        />
        <Metric
          name="cr.node.sys.host.net.send.drop"
          title="Send Drops"
          sources={nodeSources}
          tenantSource={tenantSource}
          perSource
          sourceDisplayNames={nodeDisplayNameByID}
          nonNegativeRate
        />
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
