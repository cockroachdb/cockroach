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
  const { storeSources, tenantSource } = props;

  return [
      <LineGraph
          title="Replicated Time"
          sources={storeSources}
          tenantSource={tenantSource}
          tooltip={`The replicated time of the replication stream, in nanoseconds since the Unix Epoch.`}
      >
          <Axis units={AxisUnits.Duration} label="Nano Seconds since Unix Epoch">
              <Metric
                  name="physical_replication.replicated_time_seconds"
                  title="Replicated Time"
                  scale={1_000_000_000}
              />
              <Metric
                  name="physical_replication.current_time_seconds"
                  title="Current Time"
                  scale={1_000_000_000}
              />
          </Axis>
      </LineGraph>,
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
