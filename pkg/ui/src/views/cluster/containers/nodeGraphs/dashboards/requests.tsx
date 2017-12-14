import React from "react";

import { LineGraph } from "src/views/cluster/components/linegraph";
import { Metric, Axis } from "src/views/shared/components/metricQuery";
import { ChartConfig } from "src/util/charts";

import { GraphDashboardProps } from "./dashboardUtils";

export const charts: ChartConfig = {
  "nodes.requests.0": {
    type: "metrics",
    measure: "count",
    metrics: [
      { name: "cr.node.requests.slow.distsender", title: "Slow Distsender Requests" },
    ],
  },
  "nodes.requests.1": {
    type: "metrics",
    measure: "count",
    metrics: [
      { name: "cr.store.requests.slow.raft", title: "Slow Raft Proposals" },
    ],
  },
  "nodes.requests.2": {
    type: "metrics",
    measure: "count",
    metrics: [
      { name: "cr.store.requests.slow.lease", title: "Slow Lease Acquisitions" },
    ],
  },
  "nodes.requests.3": {
    type: "metrics",
    measure: "count",
    metrics: [
      { name: "cr.store.requests.slow.commandqueue", title: "Slow Command Queue Entries" },
    ],
  },
};

export default function (props: GraphDashboardProps) {
  const { storeSources } = props;

  return [
    <LineGraph title="Slow Raft Proposals" sources={storeSources}>
      <Axis label="proposals">
        <Metric name="cr.store.requests.slow.raft" title="Slow Raft Proposals" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph title="Slow Lease Acquisitions" sources={storeSources}>
      <Axis label="lease acquisitions">
        <Metric name="cr.store.requests.slow.lease" title="Slow Lease Acquisitions" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph title="Slow Command Queue Entries" sources={storeSources}>
      <Axis label="queue entries">
        <Metric name="cr.store.requests.slow.commandqueue" title="Slow Command Queue Entries" nonNegativeRate />
      </Axis>
    </LineGraph>,
  ];
}
