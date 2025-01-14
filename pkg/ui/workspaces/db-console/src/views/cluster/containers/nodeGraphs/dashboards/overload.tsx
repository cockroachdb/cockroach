// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { AxisUnits } from "@cockroachlabs/cluster-ui";
import React from "react";

import LineGraph from "src/views/cluster/components/linegraph";
import { Metric, Axis } from "src/views/shared/components/metricQuery";

import { GraphDashboardProps, nodeDisplayName } from "./dashboardUtils";
import { storeMetrics } from "./storeUtils";

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
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Admission IO Tokens Exhausted Duration Per Second"
      sources={storeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
      tooltip={`Relative time the node had exhausted IO tokens for all IO-bound work per second of wall time, measured in microseconds/second. Increased IO token exhausted duration indicates IO resource exhaustion.`}
    >
      <Axis label="Duration (micros/sec)">
        {storeMetrics(
          {
            name: "cr.store.admission.granter.io_tokens_exhausted_duration.kv",
            nonNegativeRate: true,
            aggregateMax: true,
          },
          nodeIDs,
          storeIDsByNodeID,
          "regular (foreground)",
        )}
        {storeMetrics(
          {
            name: "cr.store.admission.granter.elastic_io_tokens_exhausted_duration.kv",
            nonNegativeRate: true,
            aggregateMax: true,
          },
          nodeIDs,
          storeIDsByNodeID,
          "elastic (background)",
        )}
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
        {storeMetrics(
          {
            name: "cr.store.admission.io.overload",
            aggregateMax: true,
          },
          nodeIDs,
          storeIDsByNodeID,
        )}
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
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
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
              title={"SQL-KV " + nodeDisplayName(nodeDisplayNameByID, nid)}
              sources={[nid]}
              downsampleMax
            />
            <Metric
              key={nid}
              name="cr.node.admission.wait_durations.sql-sql-response-p99"
              title={"SQL-SQL " + nodeDisplayName(nodeDisplayNameByID, nid)}
              sources={[nid]}
              downsampleMax
            />
          </>
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Admission Queueing Delay p99 – Store"
      sources={storeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
      tooltip={`The 99th percentile latency of requests waiting in the Admission Control store queue.`}
    >
      <Axis units={AxisUnits.Duration} label="Write Delay Duration">
        {storeMetrics(
          {
            name: "cr.store.admission.wait_durations.kv-stores-p99",
            aggregateMax: true,
          },
          nodeIDs,
          storeIDsByNodeID,
          "KV",
        )}
        {storeMetrics(
          {
            name: "cr.store.admission.wait_durations.elastic-stores-p99",
            aggregateMax: true,
          },
          nodeIDs,
          storeIDsByNodeID,
          "elastic",
        )}
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
              title={nodeDisplayName(nodeDisplayNameByID, nid)}
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
      <Axis units={AxisUnits.Duration} label="Flow Token Wait Duration">
        {nodeIDs.map(nid => (
          <>
            <Metric
              key={nid}
              name="cr.node.kvflowcontrol.eval_wait.regular.duration-p99"
              title={"Regular " + nodeDisplayName(nodeDisplayNameByID, nid)}
              sources={[nid]}
              downsampleMax
            />
            <Metric
              key={nid}
              name="cr.node.kvflowcontrol.eval_wait.elastic.duration-p99"
              title={"Elastic " + nodeDisplayName(nodeDisplayNameByID, nid)}
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
      <Axis label="Blocked Stream Count">
        {nodeIDs.map(nid => (
          <>
            <Metric
              key={nid}
              name="cr.node.kvflowcontrol.streams.eval.regular.blocked_count"
              title={"Regular " + nodeDisplayName(nodeDisplayNameByID, nid)}
              sources={[nid]}
            />
            <Metric
              key={nid}
              name="cr.node.kvflowcontrol.streams.eval.elastic.blocked_count"
              title={"Elastic " + nodeDisplayName(nodeDisplayNameByID, nid)}
              sources={[nid]}
            />
          </>
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Replication Stream Send Queue Size"
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
      tooltip={`Queued bytes to be sent across all replication streams on a node in Replication Admission Control. `}
    >
      <Axis units={AxisUnits.Bytes} label="Size">
        {nodeIDs.map(nid => (
          <>
            <Metric
              key={nid}
              name="cr.node.kvflowcontrol.send_queue.bytes"
              title={nodeDisplayName(nodeDisplayNameByID, nid)}
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
              title={nodeDisplayName(nodeDisplayNameByID, nid)}
              sources={[nid]}
            />
            <Metric
              name="cr.node.admission.elastic_cpu.utilization_limit"
              title={nodeDisplayName(nodeDisplayNameByID, nid) + " Limit"}
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
        {storeMetrics(
          {
            name: "cr.store.storage.l0-sublevels",
            aggregateMax: true,
          },
          nodeIDs,
          storeIDsByNodeID,
        )}
      </Axis>
    </LineGraph>,
  ];
}
