// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";

import LineGraph from "src/views/cluster/components/linegraph";
import { Metric, Axis } from "src/views/shared/components/metricQuery";

import { GraphDashboardProps } from "./dashboardUtils";

export default function (props: GraphDashboardProps) {
  const { storeSources, tenantSource } = props;

  return [
    <LineGraph
      title="Slow Raft Proposals"
      sources={storeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
    >
      <Axis label="proposals">
        <Metric
          name="cr.store.requests.slow.raft"
          title="Slow Raft Proposals"
          downsampleMax
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Slow DistSender RPCs"
      sources={storeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
    >
      <Axis label="proposals">
        <Metric
          name="cr.node.requests.slow.distsender"
          title="Slow DistSender RPCs"
          downsampleMax
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Slow Lease Acquisitions"
      sources={storeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
    >
      <Axis label="lease acquisitions">
        <Metric
          name="cr.store.requests.slow.lease"
          title="Slow Lease Acquisitions"
          downsampleMax
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Slow Latch Acquisitions"
      sources={storeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
    >
      <Axis label="latch acquisitions">
        <Metric
          name="cr.store.requests.slow.latch"
          title="Slow Latch Acquisitions"
          downsampleMax
        />
      </Axis>
    </LineGraph>,
  ];
}
