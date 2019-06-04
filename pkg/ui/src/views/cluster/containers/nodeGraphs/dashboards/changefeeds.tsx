// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

import React from "react";

import { LineGraph } from "src/views/cluster/components/linegraph";
import { Metric, Axis, AxisUnits } from "src/views/shared/components/metricQuery";

import { GraphDashboardProps } from "./dashboardUtils";

export default function (props: GraphDashboardProps) {
  const { storeSources } = props;

  return [
    <LineGraph title="Max Changefeed Latency" sources={storeSources}>
      <Axis units={AxisUnits.Duration} label="time">
        <Metric name="cr.node.changefeed.max_behind_nanos" title="Max Changefeed Latency" downsampleMax aggregateMax />
      </Axis>
    </LineGraph>,

    <LineGraph title="Sink Byte Traffic" sources={storeSources}>
      <Axis units={AxisUnits.Bytes} label="bytes">
        <Metric name="cr.node.changefeed.emitted_bytes" title="Emitted Bytes" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph title="Sink Counts" sources={storeSources}>
      <Axis units={AxisUnits.Count} label="actions">
        <Metric name="cr.node.changefeed.emitted_messages" title="Messages" nonNegativeRate />
        <Metric name="cr.node.changefeed.flushes" title="Flushes" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph title="Sink Timings" sources={storeSources}>
      <Axis units={AxisUnits.Duration} label="time">
        <Metric name="cr.node.changefeed.emit_nanos" title="Message Emit Time" nonNegativeRate />
        <Metric name="cr.node.changefeed.flush_nanos" title="Flush Time" nonNegativeRate />
      </Axis>
    </LineGraph>,
  ];
}
