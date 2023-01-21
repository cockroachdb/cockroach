// Copyright 2019 The Cockroach Authors.
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
import { AxisUnits } from "@cockroachlabs/cluster-ui";

import { GraphDashboardProps } from "./dashboardUtils";

export default function (props: GraphDashboardProps) {
  const { storeSources } = props;

  return [
    <LineGraph
      title="Replication Lag"
      sources={storeSources}
      tooltip={`The time between the wall clock and replicated time of the replication stream. 
          This metric tracks how far behind the replication stream is relative to now.`}
    >
      <Axis units={AxisUnits.Duration} label="time">
        <Metric
          name="cr.node.replication.frontier_lag_seconds"
          title="Replication Lag"
          downsampleMax
          aggregateMax
        />
      </Axis>
    </LineGraph>,
    <LineGraph
      title="KV Bytes"
      sources={storeSources}
      tooltip={`KV bytes (sum of keys + values) ingested by all replication jobs`}
    >
      <Axis units={AxisUnits.Duration} label="time">
        <Metric
          name="cr.node.replication.kv_bytes"
          title="KV Bytes"
          downsampleMax
          aggregateMax
        />
      </Axis>
    </LineGraph>,
    <LineGraph
      title="SST Bytes"
      sources={storeSources}
      tooltip={`SST bytes (compressed) sent to KV by all replication jobs`}
    >
      <Axis units={AxisUnits.Duration} label="time">
        <Metric
          name="cr.node.replication.sst_bytes"
          title="SST Bytes"
          downsampleMax
          aggregateMax
        />
      </Axis>
    </LineGraph>,
  ];
}
