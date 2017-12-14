import React from "react";
import _ from "lodash";

import { LineGraph } from "src/views/cluster/components/linegraph";
import { Metric, Axis, AxisUnits } from "src/views/shared/components/metricQuery";
import { ChartConfig } from "src/util/charts";

import { GraphDashboardProps, nodeDisplayName } from "./dashboardUtils";

export const charts: ChartConfig = {
  "nodes.distributed.0": {
    type: "metrics",
    measure: "count",
    metrics: [
      { name: "cr.node.distsender.batches", title: "Batches" },
      { name: "cr.node.distsender.batches.partial", title: "Partial Batches" },
    ],
  },
  "nodes.distributed.1": {
    type: "metrics",
    measure: "count",
    metrics: [
      { name: "cr.node.distsender.rpc.sent", title: "RPCs Sent" },
      { name: "cr.node.distsender.rpc.sent.local", title: "Local Fast-path" },
    ],
  },
  "nodes.distributed.2": {
    type: "metrics",
    measure: "count",
    metrics: [
      { name: "cr.node.distsender.rpc.sent.sendnexttimeout", title: "RPC Timeouts" },
      { name: "cr.node.distsender.rpc.sent.nextreplicaerror", title: "Replica Errors" },
      { name: "cr.node.distsender.errors.notleaseholder", title: "Not Leaseholder Errors" },
    ],
  },
  "nodes.distributed.3": {
    type: "metrics",
    measure: "count",
    metrics: [
      { name: "cr.node.txn.commits", title: "Committed" },
      { name: "cr.node.txn.commits1PC", title: "Fast-path Committed" },
      { name: "cr.node.txn.aborts", title: "Aborted" },
      { name: "cr.node.txn.abandons", title: "Abandoned" },
    ],
  },
  "nodes.distributed.4": {
    type: "metrics",
    measure: "count",
    metrics: [
      { name: "cr.node.txn.restarts.writetooold", title: "Write Too Old" },
      { name: "cr.node.txn.restarts.deleterange", title: "Forwarded Timestamp (delete range)" },
      { name: "cr.node.txn.restarts.serializable", title: "Forwarded Timestamp (iso=serializable)" },
      { name: "cr.node.txn.restarts.possiblereplay", title: "Possible Replay" },
    ],
  },
  "nodes.distributed.5": {
    type: "nodes",
    measure: "duration",
    metric: { name: "cr.node.txn.durations-p99" },
  },
  "nodes.distributed.6": {
    type: "nodes",
    measure: "duration",
    metric: { name: "cr.node.txn.durations-p90" },
  },
};

export default function (props: GraphDashboardProps) {
  const { nodeIDs, nodesSummary, nodeSources } = props;

  return [
    <LineGraph title="Batches" sources={nodeSources}>
      <Axis label="batches">
        <Metric name="cr.node.distsender.batches" title="Batches" nonNegativeRate />
        <Metric name="cr.node.distsender.batches.partial" title="Partial Batches" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph title="RPCs" sources={nodeSources}>
      <Axis label="rpcs">
        <Metric name="cr.node.distsender.rpc.sent" title="RPCs Sent" nonNegativeRate />
        <Metric name="cr.node.distsender.rpc.sent.local" title="Local Fast-path" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph title="RPC Errors" sources={nodeSources}>
      <Axis label="errors">
        <Metric name="cr.node.distsender.rpc.sent.sendnexttimeout" title="RPC Timeouts" nonNegativeRate />
        <Metric name="cr.node.distsender.rpc.sent.nextreplicaerror" title="Replica Errors" nonNegativeRate />
        <Metric name="cr.node.distsender.errors.notleaseholder" title="Not Leaseholder Errors" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph title="KV Transactions" sources={nodeSources}>
      <Axis label="transactions">
        <Metric name="cr.node.txn.commits" title="Committed" nonNegativeRate />
        <Metric name="cr.node.txn.commits1PC" title="Fast-path Committed" nonNegativeRate />
        <Metric name="cr.node.txn.aborts" title="Aborted" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph title="KV Transaction Restarts" sources={nodeSources}>
      <Axis label="restarts">
        <Metric name="cr.node.txn.restarts.writetooold" title="Write Too Old" nonNegativeRate />
        <Metric name="cr.node.txn.restarts.deleterange" title="Forwarded Timestamp (delete range)" nonNegativeRate />
        <Metric name="cr.node.txn.restarts.serializable" title="Forwarded Timestamp (iso=serializable)" nonNegativeRate />
        <Metric name="cr.node.txn.restarts.possiblereplay" title="Possible Replay" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph title="KV Transaction Durations: 99th percentile"
      tooltip={`The 99th percentile of transaction durations over a 1 minute period.
                              Values are displayed individually for each node.`}>
      <Axis units={AxisUnits.Duration} label="transaction duration">
        {
          _.map(nodeIDs, (node) => (
            <Metric
              key={node}
              name="cr.node.txn.durations-p99"
              title={nodeDisplayName(nodesSummary, node)}
              sources={[node]}
              downsampleMax
            />
          ))
        }
      </Axis>
    </LineGraph>,

    <LineGraph title="KV Transaction Durations: 90th percentile"
      tooltip={`The 90th percentile of transaction durations over a 1 minute period.
                              Values are displayed individually for each node.`}>
      <Axis units={AxisUnits.Duration} label="transaction duration">
        {
          _.map(nodeIDs, (node) => (
            <Metric
              key={node}
              name="cr.node.txn.durations-p90"
              title={nodeDisplayName(nodesSummary, node)}
              sources={[node]}
              downsampleMax
            />
          ))
        }
      </Axis>
    </LineGraph>,

    <LineGraph title="Node Heartbeat Latency: 99th percentile"
      tooltip={`The 99th percentile of latency to heartbeat a node's internal liveness record over a 1 minute period.
                              Values are displayed individually for each node.`}>
      <Axis units={AxisUnits.Duration} label="heartbeat latency">
        {
          _.map(nodeIDs, (node) => (
            <Metric
              key={node}
              name="cr.node.liveness.heartbeatlatency-p99"
              title={nodeDisplayName(nodesSummary, node)}
              sources={[node]}
              downsampleMax
            />
          ))
        }
      </Axis>
    </LineGraph>,

    <LineGraph title="Node Heartbeat Latency: 90th percentile"
      tooltip={`The 90th percentile of latency to heartbeat a node's internal liveness record over a 1 minute period.
                              Values are displayed individually for each node.`}>
      <Axis units={AxisUnits.Duration} label="heartbeat latency">
        {
          _.map(nodeIDs, (node) => (
            <Metric
              key={node}
              name="cr.node.liveness.heartbeatlatency-p90"
              title={nodeDisplayName(nodesSummary, node)}
              sources={[node]}
              downsampleMax
            />
          ))
        }
      </Axis>
    </LineGraph>,

  ];
}
