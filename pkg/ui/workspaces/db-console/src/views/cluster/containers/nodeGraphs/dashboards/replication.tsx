// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { AxisUnits } from "@cockroachlabs/cluster-ui";
import React from "react";

import { cockroach } from "src/js/protos";
import LineGraph from "src/views/cluster/components/linegraph";
import {
  CircuitBreakerTrippedReplicasTooltip,
  LogicalBytesGraphTooltip,
  ReceiverSnapshotsQueuedTooltip,
} from "src/views/cluster/containers/nodeGraphs/dashboards/graphTooltips";
import { Axis, Metric } from "src/views/shared/components/metricQuery";

import {
  GraphDashboardProps,
  nodeDisplayName,
  storeIDsForNode,
} from "./dashboardUtils";

import TimeSeriesQueryAggregator = cockroach.ts.tspb.TimeSeriesQueryAggregator;

export default function (props: GraphDashboardProps) {
  const {
    nodeIDs,
    storeSources,
    nodeDisplayNameByID,
    storeIDsByNodeID,
    tenantSource,
  } = props;

  return [
    <LineGraph
      title="Ranges"
      sources={storeSources}
      tenantSource={tenantSource}
      tooltip={`Various details about the status of ranges. In the node view, shows
          details about ranges the node is responsible for. In the cluster view,
          shows details about ranges all across the cluster.`}
      showMetricsInTooltip={true}
    >
      <Axis label="ranges">
        <Metric name="cr.store.ranges" title="Ranges" />
        <Metric name="cr.store.replicas.leaders" title="Leaders" />
        <Metric name="cr.store.replicas.leaseholders" title="Lease Holders" />
        <Metric
          name="cr.store.replicas.leaders_not_leaseholders"
          title="Leaders w/o Lease"
        />
        <Metric
          name="cr.store.ranges.unavailable"
          title="Unavailable"
          color="#F16969"
        />
        <Metric
          name="cr.store.ranges.underreplicated"
          title="Under-replicated"
        />
        <Metric name="cr.store.ranges.overreplicated" title="Over-replicated" />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Replicas per Node"
      tenantSource={tenantSource}
      tooltip={`The number of replicas on each node.`}
      showMetricsInTooltip={true}
    >
      <Axis label="replicas">
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
            name="cr.store.replicas"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={storeIDsForNode(storeIDsByNodeID, nid)}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Leaseholders per Node"
      tenantSource={tenantSource}
      tooltip={`The number of leaseholder replicas on each node. A leaseholder replica
          is the one that receives and coordinates all read and write requests
          for its range.`}
      showMetricsInTooltip={true}
    >
      <Axis label="leaseholders">
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
            name="cr.store.replicas.leaseholders"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={storeIDsForNode(storeIDsByNodeID, nid)}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Lease Types"
      sources={storeSources}
      tenantSource={tenantSource}
      tooltip={`Details about the types of leases in use by ranges in the
                system. A cluster is expected to have a mix of lease types.
                In the node view, shows details about leases the node is
                responsible for. In the cluster view, shows details about
                leases all across the cluster.`}
      showMetricsInTooltip={true}
    >
      <Axis label="leases">
        <Metric name="cr.store.leases.expiration" title="Expiration Leases" />
        <Metric name="cr.store.leases.epoch" title="Epoch Leases" />
        <Metric name="cr.store.leases.leader" title="Leader Leases" />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Lease Preferences"
      sources={storeSources}
      tenantSource={tenantSource}
      tooltip={`Details about the conformance of range lease preferences. In 
                the node view, shows details about ranges the node is 
                responsible for. In the cluster view, shows details about
                ranges all across the cluster.`}
      showMetricsInTooltip={true}
    >
      <Axis label="ranges">
        <Metric
          name="cr.store.leases.preferences.violating"
          title="Lease Preferences Violating"
        />
        <Metric
          name="cr.store.leases.preferences.less-preferred"
          title="Lease Preferences Less Preferred"
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Average Replica Queries per Node"
      tenantSource={tenantSource}
      tooltip={`Moving average of the number of KV batch requests processed by
          leaseholder replicas on each node per second. Tracks roughly the last
          30 minutes of requests. Used for load-based rebalancing decisions.`}
      showMetricsInTooltip={true}
    >
      <Axis label="queries">
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
            name="cr.store.rebalancing.queriespersecond"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={storeIDsForNode(storeIDsByNodeID, nid)}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Average Replica CPU per Node"
      tenantSource={tenantSource}
      tooltip={`Moving average of all replica CPU usage on each node per second.
          Tracks roughly the last 30 minutes of usage. Used for load-based
          rebalancing decisions.`}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Duration} label="CPU time">
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
            name="cr.store.rebalancing.cpunanospersecond"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={storeIDsForNode(storeIDsByNodeID, nid)}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Logical Bytes per Node"
      tenantSource={tenantSource}
      tooltip={<LogicalBytesGraphTooltip />}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Bytes} label="logical store size">
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
            name="cr.store.totalbytes"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={storeIDsForNode(storeIDsByNodeID, nid)}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Replica Quiescence"
      sources={storeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
    >
      <Axis label="replicas">
        <Metric name="cr.store.replicas" title="Replicas" />
        <Metric name="cr.store.replicas.quiescent" title="Quiescent" />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Range Operations"
      sources={storeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
    >
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

    <LineGraph
      title="Snapshots"
      sources={storeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
    >
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

    <LineGraph
      title="Snapshot Data Received"
      sources={storeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
    >
      <Axis label="bytes" units={AxisUnits.Bytes}>
        {nodeIDs.map(nid => (
          <>
            <Metric
              key={nid}
              name="cr.store.range.snapshots.rebalancing.rcvd-bytes"
              title={nodeDisplayName(nodeDisplayNameByID, nid) + "-rebalancing"}
              sources={storeIDsForNode(storeIDsByNodeID, nid)}
              nonNegativeRate
            />
            <Metric
              key={nid}
              name="cr.store.range.snapshots.recovery.rcvd-bytes"
              title={nodeDisplayName(nodeDisplayNameByID, nid) + "-recovery"}
              sources={storeIDsForNode(storeIDsByNodeID, nid)}
              nonNegativeRate
            />
            <Metric
              key={nid}
              name="cr.store.range.snapshots.upreplication.rcvd-bytes"
              title={
                nodeDisplayName(nodeDisplayNameByID, nid) + "-upreplication"
              }
              sources={storeIDsForNode(storeIDsByNodeID, nid)}
              nonNegativeRate
            />
          </>
        ))}
      </Axis>
    </LineGraph>,
    <LineGraph
      title="Receiver Snapshots Queued"
      sources={storeSources}
      tenantSource={tenantSource}
      tooltip={ReceiverSnapshotsQueuedTooltip}
      showMetricsInTooltip={true}
    >
      <Axis label="snapshots" units={AxisUnits.Count}>
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
            name="cr.store.range.snapshots.recv-queue"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={storeIDsForNode(storeIDsByNodeID, nid)}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Circuit Breaker Tripped Replicas"
      tenantSource={tenantSource}
      tooltip={CircuitBreakerTrippedReplicasTooltip}
      showMetricsInTooltip={true}
    >
      <Axis label="replicas">
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
            name="cr.store.kv.replica_circuit_breaker.num_tripped_replicas"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={storeIDsForNode(storeIDsByNodeID, nid)}
            downsampler={TimeSeriesQueryAggregator.SUM}
          />
        ))}
      </Axis>
    </LineGraph>,
    <LineGraph
      title="Replicate Queue Actions: Successes"
      sources={storeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
    >
      <Axis label="replicas" units={AxisUnits.Count}>
        <Metric
          name="cr.store.queue.replicate.addreplica.success"
          title={"Replicas Added / Sec"}
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.replicate.removereplica.success"
          title={"Replicas Removed / Sec"}
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.replicate.replacedeadreplica.success"
          title={"Dead Replicas Replaced / Sec"}
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.replicate.removedeadreplica.success"
          title={"Dead Replicas Removed / Sec"}
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.replicate.replacedecommissioningreplica.success"
          title={"Decommissioning Replicas Replaced / Sec"}
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.replicate.removedecommissioningreplica.success"
          title={"Decommissioning Replicas Removed / Sec"}
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,
    <LineGraph
      title="Replicate Queue Actions: Failures"
      sources={storeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
    >
      <Axis label="replicas" units={AxisUnits.Count}>
        <Metric
          name="cr.store.queue.replicate.addreplica.error"
          title={"Replicas Added Errors / Sec"}
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.replicate.removereplica.error"
          title={"Replicas Removed Errors / Sec"}
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.replicate.replacedeadreplica.error"
          title={"Dead Replicas Replaced Errors / Sec"}
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.replicate.removedeadreplica.error"
          title={"Dead Replicas Removed Errors / Sec"}
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.replicate.replacedecommissioningreplica.error"
          title={"Decommissioning Replicas Replaced Errors / Sec"}
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.replicate.removedecommissioningreplica.error"
          title={"Decommissioning Replicas Removed Errors / Sec"}
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,
    <LineGraph
      title="Decommissioning Errors"
      sources={storeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
    >
      <Axis label="Replaced Errors / Sec" units={AxisUnits.Count}>
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
            name="cr.store.queue.replicate.replacedecommissioningreplica.error"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={storeIDsForNode(storeIDsByNodeID, nid)}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,
  ];
}
