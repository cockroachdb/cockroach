// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { AxisUnits } from "@cockroachlabs/cluster-ui";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import React from "react";

import LineGraph from "src/views/cluster/components/linegraph";
import { Axis, Metric } from "src/views/shared/components/metricQuery";

import {
  GraphDashboardProps,
  nodeDisplayName,
  storeIDsForNode,
} from "./dashboardUtils";

import TimeSeriesQueryAggregator = cockroach.ts.tspb.TimeSeriesQueryAggregator;

export default function (props: GraphDashboardProps) {
  const {
    storeSources,
    nodeIDs,
    nodeDisplayNameByID,
    storeIDsByNodeID,
    tenantSource,
  } = props;

  return [
    <LineGraph
      title="Changefeed Status"
      isKvGraph={false}
      sources={storeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Count} label="count">
        <Metric
          name="cr.node.jobs.changefeed.currently_running"
          title="Running"
        />
        <Metric
          name="cr.node.jobs.changefeed.currently_paused"
          title="Paused"
        />
        <Metric name="cr.node.jobs.changefeed.resume_failed" title="Failed" />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Commit Latency"
      tooltip={`The difference between an event's MVCC timestamp and the time it was
          acknowledged as received by the downstream sink.`}
      showMetricsInTooltip={true}
      isKvGraph={false}
      sources={storeSources}
      tenantSource={tenantSource}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        <Metric
          name="cr.node.changefeed.commit_latency-p99"
          title="99th Percentile"
          downsampleMax
          aggregateMax
        />
        <Metric
          name="cr.node.changefeed.commit_latency-p90"
          title="90th Percentile"
          downsampleMax
          aggregateMax
        />
        <Metric
          name="cr.node.changefeed.commit_latency-p50"
          title="50th Percentile"
          downsampleMax
          aggregateMax
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Emitted Bytes"
      isKvGraph={false}
      sources={storeSources}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Bytes} label="bytes">
        <Metric
          name="cr.node.changefeed.emitted_bytes"
          title="Emitted Bytes"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Sink Counts"
      isKvGraph={false}
      sources={storeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
    >
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

    <LineGraph
      title="Max Checkpoint Lag"
      isKvGraph={false}
      tooltip={`The most any changefeed's persisted checkpoint is behind the present.
          Larger values indicate issues with successfully ingesting or emitting
          changes. If errors cause a changefeed to restart, or the changefeed is
          paused and unpaused, emitted data up to the last checkpoint may be
          re-emitted.`}
      showMetricsInTooltip={true}
      tenantSource={tenantSource}
    >
      <Axis units={AxisUnits.Duration} label="time">
        <Metric
          name="cr.node.changefeed.max_behind_nanos"
          title="Max Checkpoint Latency"
          downsampleMax
          aggregateMax
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Changefeed Restarts"
      tooltip={`The rate of transient non-fatal errors, such as temporary connectivity
          issues or a rolling upgrade. This rate constantly becoming non-zero
          may indicate a more persistent issue.`}
      showMetricsInTooltip={true}
      isKvGraph={false}
      sources={storeSources}
      tenantSource={tenantSource}
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
      title="Oldest Protected Timestamp"
      tooltip={`The oldest data that any changefeed is protecting from being able to
          be automatically garbage collected.`}
      showMetricsInTooltip={true}
      isKvGraph={false}
      sources={storeSources}
    >
      <Axis units={AxisUnits.Duration} label="time">
        <Metric
          name="cr.node.jobs.changefeed.protected_age_sec"
          title="Protected Timestamp Age"
          scale={1_000_000_000}
          downsampleMax
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Backfill Pending Ranges"
      tooltip={`The number of ranges being backfilled (ex: due to an initial scan or
          schema change) that are yet to completely enter the Changefeed
          pipeline.`}
      showMetricsInTooltip={true}
      isKvGraph={false}
      sources={storeSources}
    >
      <Axis units={AxisUnits.Count} label="count">
        <Metric
          name="cr.node.changefeed.backfill_pending_ranges"
          title="Backfill Pending Ranges"
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Schema Registry Registrations"
      tooltip={`The rate of schema registration requests made by CockroachDB nodes to
          a configured schema registry endpoint (ex: A Kafka feed pointing to a
          Confluent Schema Registry)`}
      showMetricsInTooltip={true}
      isKvGraph={false}
      sources={storeSources}
    >
      <Axis units={AxisUnits.Count} label="action">
        <Metric
          name="cr.node.changefeed.schema_registry.registrations"
          title="Schema Registry Registrations"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Ranges in catchup mode"
      isKvGraph={false}
      sources={storeSources}
      tooltip={`Total number of ranges with an active rangefeed that are performing
          catchup scan`}
      showMetricsInTooltip={true}
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
      showMetricsInTooltip={true}
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
