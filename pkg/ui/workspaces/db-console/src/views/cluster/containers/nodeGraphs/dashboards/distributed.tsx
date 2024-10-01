// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { AxisUnits } from "@cockroachlabs/cluster-ui";
import map from "lodash/map";
import React from "react";

import LineGraph from "src/views/cluster/components/linegraph";
import { Metric, Axis } from "src/views/shared/components/metricQuery";

import { GraphDashboardProps, nodeDisplayName } from "./dashboardUtils";

export default function (props: GraphDashboardProps) {
  const { nodeIDs, nodeSources, nodeDisplayNameByID, tenantSource } = props;

  return [
    <LineGraph
      title="Batches"
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
    >
      <Axis label="batches">
        <Metric
          name="cr.node.distsender.batches"
          title="Batches"
          nonNegativeRate
        />
        <Metric
          name="cr.node.distsender.batches.partial"
          title="Partial Batches"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="RPCs"
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
    >
      <Axis label="rpcs">
        <Metric
          name="cr.node.distsender.rpc.sent"
          title="RPCs Sent"
          nonNegativeRate
        />
        <Metric
          name="cr.node.distsender.rpc.sent.local"
          title="Local Fast-path"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="RPC Errors"
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
    >
      <Axis label="errors">
        <Metric
          name="cr.node.distsender.rpc.sent.sendnexttimeout"
          title="RPC Timeouts"
          nonNegativeRate
        />
        <Metric
          name="cr.node.distsender.rpc.sent.nextreplicaerror"
          title="Replica Errors"
          nonNegativeRate
        />
        <Metric
          name="cr.node.distsender.errors.notleaseholder"
          title="Not Leaseholder Errors"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="KV Transactions"
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
    >
      <Axis label="transactions">
        <Metric name="cr.node.txn.commits" title="Committed" nonNegativeRate />
        <Metric
          name="cr.node.txn.commits1PC"
          title="Fast-path Committed"
          nonNegativeRate
        />
        <Metric name="cr.node.txn.aborts" title="Aborted" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="KV Transaction Durations: 99th percentile"
      tenantSource={tenantSource}
      tooltip={`The 99th percentile of transaction durations over a 1 minute period.
          Values are displayed individually for each node.`}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Duration} label="transaction duration">
        {map(nodeIDs, node => (
          <Metric
            key={node}
            name="cr.node.txn.durations-p99"
            title={nodeDisplayName(nodeDisplayNameByID, node)}
            sources={[node]}
            downsampleMax
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="KV Transaction Durations: 90th percentile"
      tenantSource={tenantSource}
      tooltip={`The 90th percentile of transaction durations over a 1 minute period.
          Values are displayed individually for each node.`}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Duration} label="transaction duration">
        {map(nodeIDs, node => (
          <Metric
            key={node}
            name="cr.node.txn.durations-p90"
            title={nodeDisplayName(nodeDisplayNameByID, node)}
            sources={[node]}
            downsampleMax
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Node Heartbeat Latency: 99th percentile"
      tenantSource={tenantSource}
      tooltip={`The 99th percentile of latency to heartbeat a node's internal liveness
          record over a 1 minute period. Values are displayed individually for
          each node.`}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Duration} label="heartbeat latency">
        {map(nodeIDs, node => (
          <Metric
            key={node}
            name="cr.node.liveness.heartbeatlatency-p99"
            title={nodeDisplayName(nodeDisplayNameByID, node)}
            sources={[node]}
            downsampleMax
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Node Heartbeat Latency: 90th percentile"
      tenantSource={tenantSource}
      tooltip={`The 90th percentile of latency to heartbeat a node's internal liveness
          record over a 1 minute period. Values are displayed individually for
          each node.`}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Duration} label="heartbeat latency">
        {map(nodeIDs, node => (
          <Metric
            key={node}
            name="cr.node.liveness.heartbeatlatency-p90"
            title={nodeDisplayName(nodeDisplayNameByID, node)}
            sources={[node]}
            downsampleMax
          />
        ))}
      </Axis>
    </LineGraph>,
  ];
}
