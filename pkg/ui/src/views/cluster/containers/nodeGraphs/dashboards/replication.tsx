import React from "react";
import _ from "lodash";

import { LineGraph } from "src/views/cluster/components/linegraph";
import { Metric, Axis, AxisUnits } from "src/views/shared/components/metricQuery";

import { GraphDashboardProps, nodeDisplayName, storeIDsForNode } from "./dashboardUtils";

export default function (props: GraphDashboardProps) {
  const { nodeIDs, nodesSummary, storeSources, tooltipSelection } = props;

  return [
    <LineGraph 
    title="Ranges" 
    sources={storeSources}
    tooltip={(
        <div>
          {`Range information for ${tooltipSelection}:`}
          <dl>
            <dt>Ranges</dt>
            <dd>Total number of ranges</dd>
            <dt>Leaders</dt>
            <dd>The number of ranges with leaders</dd>
            <dt>Lease Holders</dt>
            <dd>The number of ranges that have leases</dd>
            <dt>Leaders w/o Lease</dt>
            <dd>The number of Raft leaders without leases</dd>
            <dt>Unavailable</dt>
            <dd>The number of unavailable ranges</dd>
            <dt>Under-replicated</dt>
            <dd>The number of under-replicated ranges</dd>
          </dl>
        </div>
      )}
    >
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

    <LineGraph title="Replica Quiescence" sources={storeSources} tooltip={`The total number of replicas and quiesced replicas. Quiesced replicas are ones that have not been accessed in a while.`}>
      <Axis label="replicas">
        <Metric name="cr.store.replicas" title="Replicas" />
        <Metric name="cr.store.replicas.quiescent" title="Quiescent" />
      </Axis>
    </LineGraph>,

    <LineGraph title="Range Operations" sources={storeSources} tooltip={`Total ranges with split, add, or remove operations.`}>
      <Axis label="ranges">
        <Metric name="cr.store.range.splits" title="Splits" nonNegativeRate />
        <Metric name="cr.store.range.adds" title="Adds" nonNegativeRate />
        <Metric name="cr.store.range.removes" title="Removes" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph
    title="Snapshots" 
    sources={storeSources} 
    tooltip={(
        <div>
          {`When a node is far behind the log file for a range, the cluster can send it a snapshot of the range and it can start following the log from there.`}
          <dl>
            <dt>Generated</dt>
            <dd>The number of snapshots created per second</dd>
            <dt>Applied (Raft-initiated)</dt>
            <dd>The number of snapshots applied to nodes per second that were initiated within Raft</dd>
            <dt>Applied (Preemptive)</dt>
            <dd>The number of snapshots applied to nodes per second that were anticipated ahead of time</dd>
            <dt>Reserved</dt>
            <dd>The number of slots reserved per second for incoming snapshots that will be sent to a node</dd>
          </dl>
        </div>
      )}
    >
      <Axis label="snapshots">
        <Metric name="cr.store.range.snapshots.generated" title="Generated" nonNegativeRate />
        <Metric name="cr.store.range.snapshots.normal-applied" title="Applied (Raft-initiated)" nonNegativeRate />
        <Metric name="cr.store.range.snapshots.preemptive-applied" title="Applied (Preemptive)" nonNegativeRate />
        <Metric name="cr.store.replicas.reserved" title="Reserved" nonNegativeRate />
      </Axis>
    </LineGraph>,
  ];
}
