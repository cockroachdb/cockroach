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
import _ from "lodash";

import { LineGraph } from "src/views/cluster/components/linegraph";
import {
  Metric,
  Axis,
  AxisUnits,
} from "src/views/shared/components/metricQuery";

import { GraphDashboardProps, nodeDisplayName } from "./dashboardUtils";

export default function (props: GraphDashboardProps) {
  const { nodeIDs, nodesSummary, nodeSources } = props;

  return [
    <LineGraph title="Batches" sources={nodeSources}>
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

    <LineGraph title="RPCs" sources={nodeSources}>
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

    <LineGraph title="RPC Errors" sources={nodeSources}>
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

    <LineGraph title="KV Transactions" sources={nodeSources}>
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
      tooltip={`The 99th percentile of transaction durations over a 1 minute period.
                              Values are displayed individually for each node.`}
    >
      <Axis units={AxisUnits.Duration} label="transaction duration">
        {_.map(nodeIDs, (node) => (
          <Metric
            key={node}
            name="cr.node.txn.durations-p99"
            title={nodeDisplayName(nodesSummary, node)}
            sources={[node]}
            downsampleMax
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="KV Transaction Durations: 90th percentile"
      tooltip={`The 90th percentile of transaction durations over a 1 minute period.
                              Values are displayed individually for each node.`}
    >
      <Axis units={AxisUnits.Duration} label="transaction duration">
        {_.map(nodeIDs, (node) => (
          <Metric
            key={node}
            name="cr.node.txn.durations-p90"
            title={nodeDisplayName(nodesSummary, node)}
            sources={[node]}
            downsampleMax
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Node Heartbeat Latency: 99th percentile"
      tooltip={`The 99th percentile of latency to heartbeat a node's internal liveness record over a 1 minute period.
                              Values are displayed individually for each node.`}
    >
      <Axis units={AxisUnits.Duration} label="heartbeat latency">
        {_.map(nodeIDs, (node) => (
          <Metric
            key={node}
            name="cr.node.liveness.heartbeatlatency-p99"
            title={nodeDisplayName(nodesSummary, node)}
            sources={[node]}
            downsampleMax
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Node Heartbeat Latency: 90th percentile"
      tooltip={`The 90th percentile of latency to heartbeat a node's internal liveness record over a 1 minute period.
                              Values are displayed individually for each node.`}
    >
      <Axis units={AxisUnits.Duration} label="heartbeat latency">
        {_.map(nodeIDs, (node) => (
          <Metric
            key={node}
            name="cr.node.liveness.heartbeatlatency-p90"
            title={nodeDisplayName(nodesSummary, node)}
            sources={[node]}
            downsampleMax
          />
        ))}
      </Axis>
    </LineGraph>,
  ];
}
