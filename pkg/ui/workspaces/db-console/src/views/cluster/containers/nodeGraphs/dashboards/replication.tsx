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

import LineGraph from "src/views/cluster/components/linegraph";
import { Axis, Metric } from "src/views/shared/components/metricQuery";
import { AxisUnits } from "@cockroachlabs/cluster-ui";

import {
  GraphDashboardProps,
  nodeDisplayName,
  storeIDsForNode,
} from "./dashboardUtils";
import {
  CircuitBreakerTrippedReplicasTooltip,
  LogicalBytesGraphTooltip,
  PausedFollowersTooltip,
  ReceiverSnapshotsQueuedTooltip,
} from "src/views/cluster/containers/nodeGraphs/dashboards/graphTooltips";
import { cockroach } from "src/js/protos";
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
      tooltip={
        <>
          Various details about the status of ranges. In the node view, shows
          details about ranges the node is responsible for. In the cluster view,
          shows details about ranges all across the cluster.
          <br />
          <br />
          Metrics:
          <ul>
            <li>cr.store.ranges</li>
            <li>cr.store.replicas.leaders</li>
            <li>cr.store.replicas.leaseholders</li>
            <li>cr.store.replicas.leaders_not_leaseholders</li>
            <li>cr.store.ranges.unavailable</li>
            <li>cr.store.ranges.underreplicated</li>
            <li>cr.store.ranges.overreplicated</li>
          </ul>
        </>
      }
    >
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
      title="Replicas per Node"
      tenantSource={tenantSource}
      tooltip={
        <>
          The number of replicas on each node.
          <br />
          <br />
          Metric: cr.store.replicas
        </>
      }
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
      tooltip={
        <>
          The number of leaseholder replicas on each node. A leaseholder replica
          is the one that receives and coordinates all read and write requests
          for its range.
          <br />
          <br />
          Metric: cr.store.replicas.leaseholders
        </>
      }
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
      title="Average Replica Queries per Node"
      tenantSource={tenantSource}
      tooltip={
        <>
          Moving average of the number of KV batch requests processed by
          leaseholder replicas on each node per second. Tracks roughly the last
          30 minutes of requests. Used for load-based rebalancing decisions.
          <br />
          <br />
          Metric: cr.store.rebalancing.queriespersecond
        </>
      }
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
      tooltip={
        <>
          Moving average of all replica CPU usage on each node per second.
          Tracks roughly the last 30 minutes of usage. Used for load-based
          rebalancing decisions.
          <br />
          <br />
          Metric: cr.store.rebalancing.cpunanospersecond
        </>
      }
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
      tooltip={
        <>
          <LogicalBytesGraphTooltip />
          Metric: cr.store.totalbytes
        </>
      }
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
      tooltip={
        <>
          Metrics:
          <ul>
            <li>cr.store.replicas</li>
            <li>cr.store.replicas.quiescent</li>
          </ul>
        </>
      }
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
      tooltip={
        <>
          Metrics:
          <ul>
            <li>cr.store.range.splits</li>
            <li>cr.store.range.merges</li>
            <li>cr.store.range.adds</li>
            <li>cr.store.range.removes</li>
            <li>cr.store.leases.transfers.success</li>
            <li>cr.store.rebalancing.lease.transfers</li>
            <li>cr.store.rebalancing.range.rebalances</li>
          </ul>
        </>
      }
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
      tooltip={
        <>
          Metrics:
          <ul>
            <li>cr.store.range.snapshots.generated</li>
            <li>cr.store.range.snapshots.applied-voter</li>
            <li>cr.store.range.snapshots.applied-initial</li>
            <li>cr.store.range.snapshots.applied-non-voter</li>
            <li>cr.store.replicas.reserved</li>
          </ul>
        </>
      }
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
      tooltip={
        <>
          Metrics:
          <ul>
            <li>cr.store.range.snapshots.rebalancing.rcvd-bytes</li>
            <li>cr.store.range.snapshots.recovery.rcvd-bytes</li>
          </ul>
        </>
      }
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
          </>
        ))}
      </Axis>
    </LineGraph>,
    <LineGraph
      title="Receiver Snapshots Queued"
      sources={storeSources}
      tenantSource={tenantSource}
      tooltip={
        <>
          {ReceiverSnapshotsQueuedTooltip}
          <br />
          Metric: cr.store.range.snapshots.recv-queue
        </>
      }
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
      tooltip={
        <>
          {CircuitBreakerTrippedReplicasTooltip}
          <br />
          Metric: cr.store.kv.replica_circuit_breaker.num_tripped_replicas
        </>
      }
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
      title="Paused Followers"
      sources={storeSources}
      tenantSource={tenantSource}
      tooltip={
        <>
          {PausedFollowersTooltip}
          <br />
          Metric: cr.store.admission.raft.paused_replicas
        </>
      }
    >
      <Axis label="replicas">
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
            name="cr.store.admission.raft.paused_replicas"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={storeIDsForNode(storeIDsByNodeID, nid)}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,
    <LineGraph
      title="Replicate Queue Actions: Successes"
      sources={storeSources}
      tenantSource={tenantSource}
      tooltip={
        <>
          Metrics:
          <ul>
            <li>cr.store.queue.replicate.addreplica.success</li>
            <li>cr.store.queue.replicate.removereplica.success</li>
            <li>cr.store.queue.replicate.replacedeadreplica.success</li>
            <li>cr.store.queue.replicate.removedeadreplica.success</li>
            <li>
              cr.store.queue.replicate.replacedecommissioningreplica.success
            </li>
            <li>
              cr.store.queue.replicate.removedecommissioningreplica.success
            </li>
          </ul>
        </>
      }
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
      tooltip={
        <>
          Metrics:
          <ul>
            <li>cr.store.queue.replicate.addreplica.error</li>
            <li>cr.store.queue.replicate.removereplica.error</li>
            <li>cr.store.queue.replicate.replacedeadreplica.error</li>
            <li>cr.store.queue.replicate.removedeadreplica.error</li>
            <li>
              cr.store.queue.replicate.replacedecommissioningreplica.error
            </li>
            <li>cr.store.queue.replicate.removedecommissioningreplica.error</li>
          </ul>
        </>
      }
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
      tooltip={
        <>
          Metric: cr.store.queue.replicate.replacedecommissioningreplica.error
        </>
      }
    >
      <Axis label="replicas" units={AxisUnits.Count}>
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
            name="cr.store.queue.replicate.replacedecommissioningreplica.error"
            title={
              nodeDisplayName(nodeDisplayNameByID, nid) +
              " - Replaced Errors / Sec"
            }
            sources={storeIDsForNode(storeIDsByNodeID, nid)}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,
  ];
}
