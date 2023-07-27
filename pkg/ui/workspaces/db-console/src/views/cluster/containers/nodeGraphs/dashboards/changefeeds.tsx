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

import LineGraph from "src/views/cluster/components/linegraph";
import { Axis, Metric } from "src/views/shared/components/metricQuery";
import { AxisUnits } from "@cockroachlabs/cluster-ui";

import {
  GraphDashboardProps,
  nodeDisplayName,
  storeIDsForNode,
} from "./dashboardUtils";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import TimeSeriesQueryAggregator = cockroach.ts.tspb.TimeSeriesQueryAggregator;

export default function (props: GraphDashboardProps) {
  const { storeSources, nodeIDs, nodeDisplayNameByID, storeIDsByNodeID } =
    props;

  return [
    <LineGraph
      title="Max Changefeed Latency"
      isKvGraph={false}
      sources={storeSources}
    >
      <Axis units={AxisUnits.Duration} label="time">
        <Metric
          name="cr.node.changefeed.max_behind_nanos"
          title="Max Changefeed Latency"
          downsampleMax
          aggregateMax
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Sink Byte Traffic"
      isKvGraph={false}
      sources={storeSources}
    >
      <Axis units={AxisUnits.Bytes} label="bytes">
        <Metric
          name="cr.node.changefeed.emitted_bytes"
          title="Emitted Bytes"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph title="Sink Counts" isKvGraph={false} sources={storeSources}>
      <Axis units={AxisUnits.Count} label="actions">
        <Metric
          name="cr.node.changefeed.emitted_messages"
          title="Messages"
          nonNegativeRate
        />
        <Metric
          name="cr.node.changefeed.flushes"
          title="Flushes"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph title="Sink Timings" isKvGraph={false} sources={storeSources}>
      <Axis units={AxisUnits.Duration} label="time">
        <Metric
          name="cr.node.changefeed.emit_nanos"
          title="Message Emit Time"
          nonNegativeRate
        />
        <Metric
          name="cr.node.changefeed.flush_nanos"
          title="Flush Time"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Changefeed Restarts"
      isKvGraph={false}
      sources={storeSources}
    >
      <Axis units={AxisUnits.Count} label="actions">
        <Metric
          name="cr.node.changefeed.error_retries"
          title="Retryable Errors"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Ranges in catchup mode"
      isKvGraph={false}
      sources={storeSources}
      tooltip="Total number of ranges with an active rangefeed that are performing catchup scan"
    >
      <Axis units={AxisUnits.Count} label="ranges">
        <Metric
          name="cr.node.distsender.rangefeed.catchup_ranges"
          title="Ranges"
          aggregator={TimeSeriesQueryAggregator.SUM}
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="RangeFeed catchup scans duration"
      isKvGraph={false}
      sources={storeSources}
    >
      <Axis units={AxisUnits.Duration} label="duration">
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
            name="cr.store.kv.rangefeed.catchup_scan_nanos"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={storeIDsForNode(storeIDsByNodeID, nid)}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,
  ];
}
