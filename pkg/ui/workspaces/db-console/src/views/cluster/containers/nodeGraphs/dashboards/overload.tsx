// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { AxisUnits } from "@cockroachlabs/cluster-ui";

import LineGraph from "src/views/cluster/components/linegraph";
import { Metric, Axis } from "src/views/shared/components/metricQuery";

import {
  GraphDashboardProps,
  nodeDisplayName,
  storeIDsForNode,
} from "./dashboardUtils";

export default function (props: GraphDashboardProps) {
  const {
    nodeIDs,
    nodeSources,
    storeSources,
    nodeDisplayNameByID,
    storeIDsByNodeID,
    tenantSource,
  } = props;

  return [
    <LineGraph
      title="CPU Utilization"
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
      tooltip={`CPU utilization of the CockroachDB process as measured by the host, displayed per node.`}
    >
      <Axis units={AxisUnits.Percentage} label="CPU Utilization">
        {nodeIDs.map(nid => (
          <Metric
            name="cr.node.sys.cpu.combined.percent-normalized"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="KV Admission CPU Slots Exhausted Duration Per Second"
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
      tooltip={`Relative time the node had exhausted slots for foreground (regular) CPU work per second of wall time, measured in microseconds/second. Increased slot exhausted duration indicates CPU resource exhaustion.`}
    >
      <Axis label="Duration (micros/sec)">
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
            name="cr.node.admission.granter.slots_exhausted_duration.kv"
            title={
              "Admission Slots Exhausted " +
              nodeDisplayName(nodeDisplayNameByID, nid)
            }
            sources={[nid]}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Admission IO Tokens Exhausted Duration Per Second"
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
      tooltip={`Relative time the node had exhausted IO tokens for all IO-bound work per second of wall time, measured in microseconds/second. Increased IO token exhausted duration indicates IO resource exhaustion.`}
    >
      <Axis label="Duration (micros/sec)">
        {nodeIDs.map(nid => (
          <>
            <Metric
              key={nid}
              name="cr.node.admission.granter.io_tokens_exhausted_duration.kv"
              title={
                "Regular (Foreground) IO Exhausted " +
                nodeDisplayName(nodeDisplayNameByID, nid)
              }
              sources={[nid]}
              nonNegativeRate
            />
            <Metric
              key={nid}
              name="cr.node.admission.granter.elastic_io_tokens_exhausted_duration.kv"
              title={
                "Elastic (Background) IO Exhausted " +
                nodeDisplayName(nodeDisplayNameByID, nid)
              }
              sources={[nid]}
              nonNegativeRate
            />
          </>
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="IO Overload"
      sources={storeSources}
      tenantSource={tenantSource}
      tooltip={`A derived score based on Admission Control's view of the store. Admission Control attempts to maintain a score of 0.5.`}
      showMetricsInTooltip={true}
    >
      <Axis label="Score">
        {nodeIDs.map(nid => (
          <>
            <Metric
              key={nid}
              name="cr.store.admission.io.overload"
              title={"IO Overload " + nodeDisplayName(nodeDisplayNameByID, nid)}
              sources={storeIDsForNode(storeIDsByNodeID, nid)}
            />
          </>
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Elastic CPU Tokens Exhausted Duration Per Second"
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`Relative time the node had exhausted tokens for background (elastic) CPU work per second of wall time, measured in microseconds/second. Increased token exhausted duration indicates CPU resource exhaustion, specifically for background (elastic) work.`}
      showMetricsInTooltip={true}
    >
      <Axis label="Duration (micros/sec)">
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
            name="cr.node.admission.elastic_cpu.nanos_exhausted_duration"
            title={
              "Elastic CPU Exhausted " +
              nodeDisplayName(nodeDisplayNameByID, nid)
            }
            sources={[nid]}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Admission Queueing Delay p99 – Foreground (Regular) CPU"
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
      tooltip={`The 99th percentile latency of requests waiting in the various Admission Control CPU queues.`}
    >
      <Axis units={AxisUnits.Duration} label="Delay Duration">
        {nodeIDs.map(nid => (
          <>
            <Metric
              key={nid}
              name="cr.node.admission.wait_durations.kv-p99"
              title={"KV " + nodeDisplayName(nodeDisplayNameByID, nid)}
              sources={[nid]}
              downsampleMax
            />
            <Metric
              key={nid}
              name="cr.node.admission.wait_durations.sql-kv-response-p99"
              title={
                "SQL-KV response " + nodeDisplayName(nodeDisplayNameByID, nid)
              }
              sources={[nid]}
              downsampleMax
            />
            <Metric
              key={nid}
              name="cr.node.admission.wait_durations.sql-sql-response-p99"
              title={
                "SQL-SQL response " + nodeDisplayName(nodeDisplayNameByID, nid)
              }
              sources={[nid]}
              downsampleMax
            />
          </>
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Admission Queueing Delay p99 – Store"
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
      tooltip={`The 99th percentile latency of requests waiting in the Admission Control store queue.`}
    >
      <Axis units={AxisUnits.Duration} label="Delay Duration">
        {nodeIDs.map(nid => (
          <>
            <Metric
              key={nid}
              name="cr.node.admission.wait_durations.kv-stores-p99"
              title={"KV write " + nodeDisplayName(nodeDisplayNameByID, nid)}
              sources={[nid]}
              downsampleMax
            />
            <Metric
              key={nid}
              name="cr.node.admission.wait_durations.elastic-stores-p99"
              title={
                "elastic (background) write " +
                nodeDisplayName(nodeDisplayNameByID, nid)
              }
              sources={[nid]}
              downsampleMax
            />
          </>
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Admission Queueing Delay p99 – Background (Elastic) CPU"
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
      tooltip={`The 99th percentile latency of requests waiting in the Admission Control elastic CPU queue.`}
    >
      <Axis units={AxisUnits.Duration} label="Delay Duration">
        {nodeIDs.map(nid => (
          <>
            <Metric
              key={nid}
              name="cr.node.admission.wait_durations.elastic-cpu-p99"
              title={"KV write " + nodeDisplayName(nodeDisplayNameByID, nid)}
              sources={[nid]}
              downsampleMax
            />
          </>
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Admission Queueing Delay p99 – Replication Admission Control"
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
      tooltip={`The 99th percentile latency of requests waiting in the Replication Admission Control queue. This metric is indicative of store overload on replicas.`}
    >
      <Axis units={AxisUnits.Duration} label="Wait Duration">
        {nodeIDs.map(nid => (
          <>
            <Metric
              key={nid}
              name="cr.node.kvadmission.flow_controller.regular_wait_duration-p99"
              title={
                "Regular flow token wait time " +
                nodeDisplayName(nodeDisplayNameByID, nid)
              }
              sources={[nid]}
              downsampleMax
            />
            <Metric
              key={nid}
              name="cr.node.kvadmission.flow_controller.elastic_wait_duration-p99"
              title={
                "Elastic flow token wait time " +
                nodeDisplayName(nodeDisplayNameByID, nid)
              }
              sources={[nid]}
              downsampleMax
            />
          </>
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Blocked Replication Streams"
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
      tooltip={`Blocked replication streams per node in Replication Admission Control, separated by admission priority {regular, elastic}.`}
    >
      <Axis label="Count">
        {nodeIDs.map(nid => (
          <>
            <Metric
              key={nid}
              name="cr.node.kvadmission.flow_controller.regular_blocked_stream_count"
              title={
                "Blocked regular streams " +
                nodeDisplayName(nodeDisplayNameByID, nid)
              }
              sources={[nid]}
            />
            <Metric
              key={nid}
              name="cr.node.kvadmission.flow_controller.elastic_blocked_stream_count"
              title={
                "Blocked elastic streams " +
                nodeDisplayName(nodeDisplayNameByID, nid)
              }
              sources={[nid]}
            />
          </>
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Elastic CPU Utilization"
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`CPU utilization by elastic work, compared to the limit set for elastic work.`}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Percentage} label="CPU Utilization">
        {nodeIDs.map(nid => (
          <>
            <Metric
              name="cr.node.admission.elastic_cpu.utilization"
              title={
                "Elastic CPU Utilization " +
                nodeDisplayName(nodeDisplayNameByID, nid)
              }
              sources={[nid]}
            />
            <Metric
              name="cr.node.admission.elastic_cpu.utilization_limit"
              title={
                "Elastic CPU Utilization Limit " +
                nodeDisplayName(nodeDisplayNameByID, nid)
              }
              sources={[nid]}
            />
          </>
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Goroutine Scheduling Latency: 99th percentile"
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`P99 scheduling latency for goroutines. A value above 1ms here indicates high load that causes background (elastic) CPU work to be throttled.`}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {nodeIDs.map(nid => (
          <>
            <Metric
              key={nid}
              name="cr.node.go.scheduler_latency-p99"
              title={nodeDisplayName(nodeDisplayNameByID, nid)}
              sources={[nid]}
              downsampleMax
            />
          </>
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Goroutine Scheduling Latency: 99.9th percentile"
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`P99.9 scheduling latency for goroutines. A high value here can be indicative of high tail latency in various queries.`}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {nodeIDs.map(nid => (
          <>
            <Metric
              key={nid}
              name="cr.node.go.scheduler_latency-p99.9"
              title={nodeDisplayName(nodeDisplayNameByID, nid)}
              sources={[nid]}
              downsampleMax
            />
          </>
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Runnable Goroutines per CPU"
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`The number of Goroutines waiting per CPU. A value above the value set in admission.kv_slot_adjuster.overload_threshold (sampled at 1ms) is used by admission control to throttle regular CPU work.`}
      showMetricsInTooltip={true}
    >
      <Axis label="goroutines">
        {nodeIDs.map(nid => (
          <Metric
            name="cr.node.sys.runnable.goroutines.per.cpu"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="LSM L0 Sublevels"
      sources={storeSources}
      tenantSource={tenantSource}
      tooltip={`Number of sublevels in L0 of the LSM. A sustained value above 10 typically indicates that the store is overloaded.`}
      showMetricsInTooltip={true}
    >
      <Axis label="Count">
        {nodeIDs.map(nid => (
          <>
            <Metric
              key={nid}
              name="cr.store.storage.l0-sublevels"
              title={
                "L0 Sublevels " + nodeDisplayName(nodeDisplayNameByID, nid)
              }
              sources={storeIDsForNode(storeIDsByNodeID, nid)}
            />
          </>
        ))}
      </Axis>
    </LineGraph>,
  ];
}
