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

import {
  GraphDashboardProps,
  nodeDisplayName,
  storeIDsForNode,
} from "./dashboardUtils";
import { LogicalBytesGraphTooltip } from "src/views/cluster/containers/nodeGraphs/dashboards/graphTooltips";

export default function (props: GraphDashboardProps) {
  const { nodeIDs, nodesSummary, storeSources } = props;

  return [
    <LineGraph title="Ranges" sources={storeSources}>
      <Axis label="ranges">
        <Metric name="cr.store.ranges" title="Ranges" />
        <Metric name="cr.store.replicas.leaders" title="Leaders" />
        <Metric name="cr.store.replicas.leaseholders" title="Lease Holders" />
        <Metric
          name="cr.store.replicas.leaders_not_leaseholders"
          title="Leaders w/o Lease"
        />
        <Metric name="cr.store.ranges.unavailable" title="Unavailable" />
        <Metric
          name="cr.store.ranges.underreplicated"
          title="Under-replicated"
        />
        <Metric name="cr.store.ranges.overreplicated" title="Over-replicated" />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Replicas per Store"
      tooltip={`The number of replicas on each store.`}
    >
      <Axis label="replicas">
        {_.map(nodeIDs, (nid) => (
          <Metric
            key={nid}
            name="cr.store.replicas"
            title={nodeDisplayName(nodesSummary, nid)}
            sources={storeIDsForNode(nodesSummary, nid)}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Leaseholders per Store"
      tooltip={`The number of leaseholder replicas on each store. A leaseholder replica is the one that
          receives and coordinates all read and write requests for its range.`}
    >
      <Axis label="leaseholders">
        {_.map(nodeIDs, (nid) => (
          <Metric
            key={nid}
            name="cr.store.replicas.leaseholders"
            title={nodeDisplayName(nodesSummary, nid)}
            sources={storeIDsForNode(nodesSummary, nid)}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Average Queries per Store"
      tooltip={`Exponentially weighted moving average of the number of KV batch requests processed by leaseholder replicas on each store per second. Tracks roughly the last 30 minutes of requests. Used for load-based rebalancing decisions.`}
    >
      <Axis label="queries">
        {_.map(nodeIDs, (nid) => (
          <Metric
            key={nid}
            name="cr.store.rebalancing.queriespersecond"
            title={nodeDisplayName(nodesSummary, nid)}
            sources={storeIDsForNode(nodesSummary, nid)}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Logical Bytes per Store"
      tooltip={<LogicalBytesGraphTooltip />}
    >
      <Axis units={AxisUnits.Bytes} label="logical store size">
        {_.map(nodeIDs, (nid) => (
          <Metric
            key={nid}
            name="cr.store.totalbytes"
            title={nodeDisplayName(nodesSummary, nid)}
            sources={storeIDsForNode(nodesSummary, nid)}
          />
        ))}
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
        <Metric name="cr.store.range.merges" title="Merges" nonNegativeRate />
        <Metric name="cr.store.range.adds" title="Adds" nonNegativeRate />
        <Metric name="cr.store.range.removes" title="Removes" nonNegativeRate />
        <Metric
          name="cr.store.leases.transfers.success"
          title="Lease Transfers"
          nonNegativeRate
        />
        <Metric
          name="cr.store.rebalancing.lease.transfers"
          title="Load-based Lease Transfers"
          nonNegativeRate
        />
        <Metric
          name="cr.store.rebalancing.range.rebalances"
          title="Load-based Range Rebalances"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph title="Snapshots" sources={storeSources}>
      <Axis label="snapshots">
        <Metric
          name="cr.store.range.snapshots.generated"
          title="Generated"
          nonNegativeRate
        />
        <Metric
          name="cr.store.range.snapshots.applied-voter"
          title="Applied (Voters)"
          nonNegativeRate
        />
        <Metric
          name="cr.store.range.snapshots.applied-initial"
          title="Applied (Initial Upreplication)"
          nonNegativeRate
        />
        <Metric
          name="cr.store.range.snapshots.applied-non-voter"
          title="Applied (Non-Voters)"
          nonNegativeRate
        />
        <Metric
          name="cr.store.replicas.reserved"
          title="Reserved"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,
  ];
}
