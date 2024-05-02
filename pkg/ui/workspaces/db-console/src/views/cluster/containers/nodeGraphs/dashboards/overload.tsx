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

import LineGraph from "src/views/cluster/components/linegraph";
import { Metric, Axis } from "src/views/shared/components/metricQuery";
import { AxisUnits } from "@cockroachlabs/cluster-ui";

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
      tooltip={`CPU utilization as measured by the host, displayed per node.`}
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
      tooltip={`Duration of CPU slots exhaustion regular work, in microseconds. This metric indicates whether the CPU is overloaded and how long do we experience token exhaustion for regular work.`}
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
      title="KV Admission IO Tokens Exhausted Duration Per Second"
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
      tooltip={`Duration of IO token exhaustion, in microseconds per second. This metric indicates whether the disk is overloaded and how long do we experience token exhaustion.`}
    >
      <Axis label="Duration (micros/sec)">
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
            name="cr.node.admission.granter.io_tokens_exhausted_duration.kv"
            title={"IO Exhausted " + nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="IO Overload"
      sources={storeSources}
      tenantSource={tenantSource}
      tooltip={`A 1-normalized IO overload score. A value above 1 indicates that the store is overloaded.`}
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
      tooltip={`Duration of CPU token exhaustion by elastic work, in microseconds per second. This metric indicates whether the CPU is overloaded and how long do we experience token exhaustion for elastic work.`}
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
      title="Flow Tokens Wait Time: 99th percentile"
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
      tooltip={`Duration of flow token wait time. This metric is indicative of token exhaustion in Replication Admission Control and shows how long requests waited for tokens.`}
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
      title="Admission Delay: 99th percentile"
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
      tooltip={`Wait duration for requests that waited in the various admission queues.`}
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
              name="cr.node.admission.wait_durations.kv-stores-p99"
              title={"KV write " + nodeDisplayName(nodeDisplayNameByID, nid)}
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
      tooltip={`P99 scheduling latency for goroutines. A value above 1ms typically indicates high load.`}
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
      tooltip={`P99.9 scheduling latency for goroutines. A value above 1ms typically indicates high load.`}
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
      tooltip={`The number of Goroutines waiting per CPU. A value above 32 typically indicates high load.`}
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
      tooltip={`Number of sublevels in L0 of the LSM. A value above 20 typically indicates that the store is overloaded.`}
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
