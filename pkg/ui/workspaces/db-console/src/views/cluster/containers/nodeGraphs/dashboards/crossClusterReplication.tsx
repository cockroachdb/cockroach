// Copyright 2023 The Cockroach Authors.
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
          This metric tracks how far behind the replication stream is relative to now. This metric is set to 0 during the initial scan.`}
    >
      <Axis units={AxisUnits.Duration} label="time">
        <Metric
          name="cr.node.replication.frontier_lag_nanos"
          title="Replication Lag"
        />
      </Axis>
    </LineGraph>,
    <LineGraph
      title="Logical Bytes"
      sources={storeSources}
      tooltip={`Rate at which the logical bytes (sum of keys + values) are ingested by all replication jobs`}
    >
      <Axis units={AxisUnits.Bytes} label="bytes">
        <Metric
          name="cr.node.replication.logical_bytes"
          title="Logical Bytes"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,
    <LineGraph
      title="SST Bytes"
      sources={storeSources}
      tooltip={`Rate at which the SST bytes (compressed) are sent to KV by all replication jobs`}
    >
      <Axis units={AxisUnits.Bytes} label="bytes">
        <Metric
          name="cr.node.replication.sst_bytes"
          title="SST Bytes"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,
  ];
}
