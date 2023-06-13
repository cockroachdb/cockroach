// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";

import LineGraph from "src/views/cluster/components/linegraph";
import { Axis, Metric } from "src/views/shared/components/metricQuery";

import {
  GraphDashboardProps,
  nodeDisplayName,
  storeIDsForNode,
} from "./dashboardUtils";
import { AxisUnits } from "@cockroachlabs/cluster-ui";

export default function (props: GraphDashboardProps) {
  const { nodeIDs, nodeDisplayNameByID, storeIDsByNodeID, tenantSource } =
    props;

  return [
    <LineGraph title="Network Bytes Received">
      <Axis units={AxisUnits.Bytes} label="bytes">
        {nodeIDs.map(nid => (
          <Metric
            name="cr.node.sys.host.net.recv.bytes"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={storeIDsForNode(storeIDsByNodeID, nid)}
            tenantSource={tenantSource}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph title="Network Bytes Sent">
      <Axis units={AxisUnits.Bytes} label="bytes">
        {nodeIDs.map(nid => (
          <Metric
            name="cr.node.sys.host.net.send.bytes"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={storeIDsForNode(storeIDsByNodeID, nid)}
            tenantSource={tenantSource}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="RPC Heartbeat Latency: 50th percentile"
      isKvGraph={false}
      tooltip={`Round-trip latency for recent successful outgoing heartbeats.`}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {nodeIDs.map(nid => (
          <Metric
            name="cr.node.round-trip-latency-p50"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={storeIDsForNode(storeIDsByNodeID, nid)}
            tenantSource={tenantSource}
            downsampleMax
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="RPC Heartbeat Latency: 99th percentile"
      isKvGraph={false}
      tooltip={`Round-trip latency for recent successful outgoing heartbeats.`}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {nodeIDs.map(nid => (
          <Metric
            name="cr.node.round-trip-latency-p99"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={storeIDsForNode(storeIDsByNodeID, nid)}
            tenantSource={tenantSource}
            downsampleMax
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Unhealthy RPC Connections"
      tooltip={`The number of outgoing connections on each node that are in an unhealthy state.`}
    >
      <Axis label="connections">
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
            name="cr.node.rpc.connection.unhealthy"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={storeIDsForNode(storeIDsByNodeID, nid)}
            tenantSource={tenantSource}
          />
        ))}
      </Axis>
    </LineGraph>,
  ];
}
