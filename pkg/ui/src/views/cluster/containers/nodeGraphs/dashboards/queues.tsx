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

import { LineGraph } from "src/views/cluster/components/linegraph";
import {
  Metric,
  Axis,
  AxisUnits,
} from "src/views/shared/components/metricQuery";

import { GraphDashboardProps } from "./dashboardUtils";

export default function (props: GraphDashboardProps) {
  const { storeSources } = props;

  return [
    <LineGraph title="Queue Processing Failures" sources={storeSources}>
      <Axis units={AxisUnits.Count} label="failures">
        <Metric
          name="cr.store.queue.gc.process.failure"
          title="GC"
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.replicagc.process.failure"
          title="Replica GC"
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.replicate.process.failure"
          title="Replication"
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.split.process.failure"
          title="Split"
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.consistency.process.failure"
          title="Consistency"
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.raftlog.process.failure"
          title="Raft Log"
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.raftsnapshot.process.failure"
          title="Raft Snapshot"
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.tsmaintenance.process.failure"
          title="Time Series Maintenance"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph title="Queue Processing Times" sources={storeSources}>
      <Axis units={AxisUnits.Duration} label="processing time">
        <Metric
          name="cr.store.queue.gc.processingnanos"
          title="GC"
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.replicagc.processingnanos"
          title="Replica GC"
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.replicate.processingnanos"
          title="Replication"
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.split.processingnanos"
          title="Split"
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.consistency.processingnanos"
          title="Consistency"
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.raftlog.processingnanos"
          title="Raft Log"
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.raftsnapshot.processingnanos"
          title="Raft Snapshot"
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.tsmaintenance.processingnanos"
          title="Time Series Maintenance"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    // TODO(mrtracy): The queues below should also have "processing
    // nanos" on the graph, but that has a time unit instead of a count
    // unit, and thus we need support for multi-axis graphs.
    <LineGraph title="Replica GC Queue" sources={storeSources}>
      <Axis units={AxisUnits.Count} label="actions">
        <Metric
          name="cr.store.queue.replicagc.process.success"
          title="Successful Actions / sec"
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.replicagc.pending"
          title="Pending Actions"
          downsampleMax
        />
        <Metric
          name="cr.store.queue.replicagc.removereplica"
          title="Replicas Removed / sec"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph title="Replication Queue" sources={storeSources}>
      <Axis units={AxisUnits.Count} label="actions">
        <Metric
          name="cr.store.queue.replicate.process.success"
          title="Successful Actions / sec"
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.replicate.pending"
          title="Pending Actions"
        />
        <Metric
          name="cr.store.queue.replicate.addreplica"
          title="Replicas Added / sec"
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.replicate.removereplica"
          title="Replicas Removed / sec"
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.replicate.removedeadreplica"
          title="Dead Replicas Removed / sec"
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.replicate.removelearnerreplica"
          title="Learner Replicas Removed / sec"
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.replicate.rebalancereplica"
          title="Replicas Rebalanced / sec"
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.replicate.transferlease"
          title="Leases Transferred / sec"
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.replicate.purgatory"
          title="Replicas in Purgatory"
          downsampleMax
        />
      </Axis>
    </LineGraph>,

    <LineGraph title="Split Queue" sources={storeSources}>
      <Axis units={AxisUnits.Count} label="actions">
        <Metric
          name="cr.store.queue.split.process.success"
          title="Successful Actions / sec"
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.split.pending"
          title="Pending Actions"
          downsampleMax
        />
      </Axis>
    </LineGraph>,

    <LineGraph title="Merge Queue" sources={storeSources}>
      <Axis units={AxisUnits.Count} label="actions">
        <Metric
          name="cr.store.queue.merge.process.success"
          title="Successful Actions / sec"
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.merge.pending"
          title="Pending Actions"
          downsampleMax
        />
      </Axis>
    </LineGraph>,

    <LineGraph title="GC Queue" sources={storeSources}>
      <Axis units={AxisUnits.Count} label="actions">
        <Metric
          name="cr.store.queue.gc.process.success"
          title="Successful Actions / sec"
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.gc.pending"
          title="Pending Actions"
          downsampleMax
        />
      </Axis>
    </LineGraph>,

    <LineGraph title="Raft Log Queue" sources={storeSources}>
      <Axis units={AxisUnits.Count} label="actions">
        <Metric
          name="cr.store.queue.raftlog.process.success"
          title="Successful Actions / sec"
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.raftlog.pending"
          title="Pending Actions"
          downsampleMax
        />
      </Axis>
    </LineGraph>,

    <LineGraph title="Raft Snapshot Queue" sources={storeSources}>
      <Axis units={AxisUnits.Count} label="actions">
        <Metric
          name="cr.store.queue.raftsnapshot.process.success"
          title="Successful Actions / sec"
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.raftsnapshot.pending"
          title="Pending Actions"
          downsampleMax
        />
      </Axis>
    </LineGraph>,

    <LineGraph title="Consistency Checker Queue" sources={storeSources}>
      <Axis units={AxisUnits.Count} label="actions">
        <Metric
          name="cr.store.queue.consistency.process.success"
          title="Successful Actions / sec"
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.consistency.pending"
          title="Pending Actions"
          downsampleMax
        />
      </Axis>
    </LineGraph>,

    <LineGraph title="Time Series Maintenance Queue" sources={storeSources}>
      <Axis units={AxisUnits.Count} label="actions">
        <Metric
          name="cr.store.queue.tsmaintenance.process.success"
          title="Successful Actions / sec"
          nonNegativeRate
        />
        <Metric
          name="cr.store.queue.tsmaintenance.pending"
          title="Pending Actions"
          downsampleMax
        />
      </Axis>
    </LineGraph>,
  ];
}
