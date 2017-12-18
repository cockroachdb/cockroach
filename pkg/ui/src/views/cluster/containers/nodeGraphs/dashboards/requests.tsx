import React from "react";

import { LineGraph } from "src/views/cluster/components/linegraph";
import { Metric, Axis, AxisUnits } from "src/views/shared/components/metricQuery";

import { GraphDashboardProps } from "./dashboardUtils";

export default function (props: GraphDashboardProps) {
  const { storeSources, nodeSources } = props;

  return [
    <LineGraph title="Slow Distsender Requests" sources={nodeSources}>
      <Axis units={AxisUnits.Count} label="requests">
        <Metric name="cr.node.requests.slow.distsender" title="Slow Distsender Requests" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph title="Slow Raft Proposals" sources={storeSources}>
      <Axis units={AxisUnits.Count} label="proposals">
        <Metric name="cr.store.requests.slow.raft" title="Slow Raft Proposals" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph title="Slow Lease Acquisitions" sources={storeSources}>
      <Axis units={AxisUnits.Count} label="lease acquisitions">
        <Metric name="cr.store.requests.slow.lease" title="Slow Lease Acquisitions" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph title="Slow Command Queue Entries" sources={storeSources}>
      <Axis units={AxisUnits.Count} label="queue entries">
        <Metric name="cr.store.requests.slow.commandqueue" title="Slow Command Queue Entries" nonNegativeRate />
      </Axis>
    </LineGraph>,
  ];
}
