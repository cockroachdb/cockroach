// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";

import LineGraph from "src/views/cluster/components/linegraph";
import { Metric, Axis } from "src/views/shared/components/metricQuery";
import { AxisUnits } from "@cockroachlabs/cluster-ui";

import { GraphDashboardProps } from "./dashboardUtils";

export default function (props: GraphDashboardProps) {
  const { storeSources, tenantSource } = props;

  return [
    <LineGraph
      title="Logical Bytes"
      sources={storeSources}
      tenantSource={tenantSource}
      tooltip={`Rate at which the logical bytes (sum of keys + values) are ingested by all replication jobs`}
    >
      <Axis units={AxisUnits.Bytes} label="bytes">
        <Metric
          name="cr.node.physical_replication.logical_bytes"
          title="Logical Bytes"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,
    <LineGraph
      title="SST Bytes"
      sources={storeSources}
      tenantSource={tenantSource}
      tooltip={`Rate at which the SST bytes (compressed) are sent to KV by all replication jobs`}
    >
      <Axis units={AxisUnits.Bytes} label="bytes">
        <Metric
          name="cr.node.physical_replication.sst_bytes"
          title="SST Bytes"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,
  ];
}
