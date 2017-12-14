import React from "react";
import _ from "lodash";

import { LineGraph } from "src/views/cluster/components/linegraph";
import { Metric, Axis, AxisUnits } from "src/views/shared/components/metricQuery";
import { ChartConfig } from "src/util/charts";

import { GraphDashboardProps, nodeDisplayName, storeIDsForNode } from "./dashboardUtils";

export const charts: ChartConfig = {
  "nodes.replication.0": {
    type: "metrics",
    measure: "count",
    metrics: [
      { name: "cr.store.ranges", title: "Ranges" },
      { name: "cr.store.replicas.leaders", title: "Leaders" },
      { name: "cr.store.replicas.leaseholders", title: "Lease Holders" },
      { name: "cr.store.replicas.leaders_not_leaseholders", title: "Leaders w/o Lease" },
      { name: "cr.store.ranges.unavailable", title: "Unavailable" },
      { name: "cr.store.ranges.underreplicated", title: "Under-replicated" },
    ],
  },
  "nodes.replication.1": {
    type: "nodes",
    measure: "count",
    metric: { name: "cr.store.replicas" },
  },
  "nodes.replication.2": {
    type: "nodes",
    measure: "count",
    metric: { name: "cr.store.replicas.leaseholders" },
  },
  "nodes.replication.3": {
    type: "nodes",
    measure: "bytes",
    metric: { name: "cr.store.totalbytes" },
  },
  "nodes.replication.4": {
    type: "nodes",
    measure: "count",
    metric: { name: "cr.store.rebalancing.writespersecond" },
  },
  "nodes.replication.5": {
    type: "metrics",
    measure: "count",
    metrics: [
      { name: "cr.store.replicas", title: "Replicas" },
      { name: "cr.store.replicas.quiescent", title: "Quiescent" },
    ],
  },
  "nodes.replication.6": {
    type: "metrics",
    measure: "count",
    metrics: [
      { name: "cr.store.range.splits", title: "Splits" },
      { name: "cr.store.range.adds", title: "Adds" },
      { name: "cr.store.range.removes", title: "Removes" },
    ],
  },
  "nodes.replication.7": {
    type: "metrics",
    measure: "count",
    metrics: [
      { name: "cr.store.range.snapshots.generated", title: "Generated" },
      { name: "cr.store.range.snapshots.normal-applied", title: "Normal-applied" },
      { name: "cr.store.range.snapshots.preemptive-applied", title: "Preemptive-applied" },
      { name: "cr.store.replicas.reserved", title: "Reserved" },
    ],
  },
};

export default function (props: GraphDashboardProps) {
  const { nodeIDs, nodesSummary, storeSources } = props;

  return [
    <LineGraph title="Ranges" sources={storeSources}>
      <Axis label="ranges">
        <Metric name="cr.store.ranges" title="Ranges" />
        <Metric name="cr.store.replicas.leaders" title="Leaders" />
        <Metric name="cr.store.replicas.leaseholders" title="Lease Holders" />
        <Metric name="cr.store.replicas.leaders_not_leaseholders" title="Leaders w/o Lease" />
        <Metric name="cr.store.ranges.unavailable" title="Unavailable" />
        <Metric name="cr.store.ranges.underreplicated" title="Under-replicated" />
      </Axis>
    </LineGraph>,

    <LineGraph title="Replicas per Store" tooltip={`The number of replicas on each store.`}>
      <Axis label="replicas">
        {
          _.map(nodeIDs, (nid) => (
            <Metric
              key={nid}
              name="cr.store.replicas"
              title={nodeDisplayName(nodesSummary, nid)}
              sources={storeIDsForNode(nodesSummary, nid)}
            />
          ))
        }
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Leaseholders per Store"
      tooltip={
          `The number of leaseholder replicas on each store. A leaseholder replica is the one that
          receives and coordinates all read and write requests for its range.`
      }
    >
      <Axis label="leaseholders">
        {
          _.map(nodeIDs, (nid) => (
            <Metric
              key={nid}
              name="cr.store.replicas.leaseholders"
              title={nodeDisplayName(nodesSummary, nid)}
              sources={storeIDsForNode(nodesSummary, nid)}
            />
          ))
        }
      </Axis>
    </LineGraph>,

    <LineGraph title="Logical Bytes per Store" tooltip={`The number of logical bytes of data on each store.`}>
      <Axis units={AxisUnits.Bytes} label="logical store size">
        {
          _.map(nodeIDs, (nid) => (
            <Metric
              key={nid}
              name="cr.store.totalbytes"
              title={nodeDisplayName(nodesSummary, nid)}
              sources={storeIDsForNode(nodesSummary, nid)}
            />
          ))
        }
      </Axis>
    </LineGraph>,

    <LineGraph title="Replica Quiescence" sources={storeSources}>
      <Axis label="replicas">
        <Metric name="cr.store.replicas" title="Replicas" />
        <Metric name="cr.store.replicas.quiescent" title="Quiescent" />
      </Axis>
    </LineGraph>,

    <LineGraph title="Range Operations" sources={storeSources}>
      <Axis label="ranges">
        <Metric name="cr.store.range.splits" title="Splits" nonNegativeRate />
        <Metric name="cr.store.range.adds" title="Adds" nonNegativeRate />
        <Metric name="cr.store.range.removes" title="Removes" nonNegativeRate />
        <Metric name="cr.store.leases.transfers.success" title="Lease Transfers" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph title="Snapshots" sources={storeSources}>
      <Axis label="snapshots">
        <Metric name="cr.store.range.snapshots.generated" title="Generated" nonNegativeRate />
        <Metric name="cr.store.range.snapshots.normal-applied" title="Applied (Raft-initiated)" nonNegativeRate />
        <Metric name="cr.store.range.snapshots.preemptive-applied" title="Applied (Preemptive)" nonNegativeRate />
        <Metric name="cr.store.replicas.reserved" title="Reserved" nonNegativeRate />
      </Axis>
    </LineGraph>,
  ];
}
