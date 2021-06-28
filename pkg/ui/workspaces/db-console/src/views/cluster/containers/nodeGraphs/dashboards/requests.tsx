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
import { Metric, Axis } from "src/views/shared/components/metricQuery";

import { GraphDashboardProps } from "./dashboardUtils";

export default function (props: GraphDashboardProps) {
  const { storeSources } = props;

  return [
    <LineGraph title="Slow Raft Proposals" sources={storeSources}>
      <Axis label="proposals">
        <Metric
          name="cr.store.requests.slow.raft"
          title="Slow Raft Proposals"
          downsampleMax
        />
      </Axis>
    </LineGraph>,

    <LineGraph title="Slow DistSender RPCs" sources={storeSources}>
      <Axis label="proposals">
        <Metric
          name="cr.node.requests.slow.distsender"
          title="Slow DistSender RPCs"
          downsampleMax
        />
      </Axis>
    </LineGraph>,

    <LineGraph title="Slow Lease Acquisitions" sources={storeSources}>
      <Axis label="lease acquisitions">
        <Metric
          name="cr.store.requests.slow.lease"
          title="Slow Lease Acquisitions"
          downsampleMax
        />
      </Axis>
    </LineGraph>,

    <LineGraph title="Slow Latch Acquisitions" sources={storeSources}>
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
