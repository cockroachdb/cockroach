// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { AxisUnits, util } from "@cockroachlabs/cluster-ui";

import LineGraph from "src/views/cluster/components/linegraph";
import { Metric, Axis } from "src/views/shared/components/metricQuery";
import { cockroach } from "src/js/protos";

import { GraphDashboardProps } from "./dashboardUtils";

import TimeSeriesQueryAggregator = cockroach.ts.tspb.TimeSeriesQueryAggregator;

export default function (props: GraphDashboardProps) {
  const { storeSources, tenantSource } = props;

  return [
    <LineGraph
      title="Logical Bytes"
      sources={storeSources}
      tenantSource={tenantSource}
      tooltip={`Rate at which the logical bytes (sum of keys + values) are ingested by all logical replication jobs`}
    >
      <Axis units={AxisUnits.Bytes} label="bytes">
        <Metric
          name="cr.node.logical_replication.logical_bytes"
          title="Logical Bytes"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,
    <LineGraph
      title="Replication Lag"
      sources={storeSources}
      tenantSource={tenantSource}
      tooltip={`Replication lag between source and destination cluster`}
    >
      <Axis units={AxisUnits.Duration} label="duration">
        <Metric
          downsampler={TimeSeriesQueryAggregator.MIN}
          aggregator={TimeSeriesQueryAggregator.MAX}
          name="cr.node.logical_replication.replicated_time_seconds"
          title="Replication Lag"
          transform={datapoints =>
            datapoints
              .filter(d => d.value !== 0)
              .map(d =>
                d.value
                  ? {
                      ...d,
                      value:
                        d.timestamp_nanos.toNumber() -
                        util.SecondsToNano(d.value),
                    }
                  : d,
              )
          }
        />
      </Axis>
    </LineGraph>,
  ];
}
