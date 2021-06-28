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

import { GraphDashboardProps } from "src/views/cluster/containers/nodeGraphs/dashboards/dashboardUtils";

export default function (props: GraphDashboardProps) {
  const { nodeSources, tooltipSelection } = props;

  return [
    <LineGraph
      title="Raft App"
      sources={nodeSources}
      tooltip={`The number of raft app messages ${tooltipSelection}`}
    >
      <Axis label="messages">
        <Metric name="cr.store.raft.rcvd.app" title="App" nonNegativeRate />
        <Metric
          name="cr.store.raft.rcvd.appresp"
          title="AppResp"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Raft Heartbeat"
      sources={nodeSources}
      tooltip={`The number of raft heartbeat messages ${tooltipSelection}`}
    >
      <Axis label="heartbeats">
        <Metric
          name="cr.store.raft.rcvd.heartbeat"
          title="Heartbeat"
          nonNegativeRate
        />
        <Metric
          name="cr.store.raft.rcvd.heartbeatresp"
          title="HeartbeatResp"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Raft Other"
      sources={nodeSources}
      tooltip={`The number of other raft messages ${tooltipSelection}`}
    >
      <Axis label="messages">
        <Metric name="cr.store.raft.rcvd.prop" title="Prop" nonNegativeRate />
        <Metric name="cr.store.raft.rcvd.vote" title="Vote" nonNegativeRate />
        <Metric
          name="cr.store.raft.rcvd.voteresp"
          title="VoteResp"
          nonNegativeRate
        />
        <Metric name="cr.store.raft.rcvd.snap" title="Snap" nonNegativeRate />
        <Metric
          name="cr.store.raft.rcvd.transferleader"
          title="TransferLeader"
          nonNegativeRate
        />
        <Metric
          name="cr.store.raft.rcvd.timeoutnow"
          title="TimeoutNow"
          nonNegativeRate
        />
        <Metric
          name="cr.store.raft.rcvd.prevote"
          title="PreVote"
          nonNegativeRate
        />
        <Metric
          name="cr.store.raft.rcvd.prevoteresp"
          title="PreVoteResp"
          nonNegativeRate
        />
        <Metric
          name="cr.store.raft.rcvd.dropped"
          title="Dropped"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Raft Time"
      sources={nodeSources}
      tooltip={`The time spent in store.processRaft() ${tooltipSelection}`}
    >
      <Axis units={AxisUnits.Duration}>
        <Metric
          name="cr.store.raft.process.workingnanos"
          title="Working"
          nonNegativeRate
        />
        <Metric
          name="cr.store.raft.process.tickingnanos"
          title="Ticking"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Raft Ticks"
      sources={nodeSources}
      tooltip={`The number of raft ticks queued ${tooltipSelection}`}
    >
      <Axis label="ticks">
        <Metric name="cr.store.raft.ticks" title="Ticks" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Pending Heartbeats"
      sources={nodeSources}
      tooltip={`The number of raft heartbeats and responses waiting to be coalesced ${tooltipSelection}`}
    >
      <Axis label="heartbeats">
        <Metric name="cr.store.raft.heartbeats.pending" title="Pending" />
      </Axis>
    </LineGraph>,
  ];
}
