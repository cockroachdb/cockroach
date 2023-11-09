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
      title="CPU Percent"
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Percentage} label="CPU">
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
      title="Goroutine Scheduling Latency: 99th percentile"
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`P99 scheduling latency for goroutines`}
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
      title="Runnable Goroutines per CPU"
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`The number of Goroutines waiting per CPU.`}
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
      title="IO Overload"
      sources={storeSources}
      tenantSource={tenantSource}
      tooltip={`The number of sublevels/files in L0 normalized by admission thresholds.`}
      showMetricsInTooltip={true}
    >
      <Axis label="IO Overload">
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
      title="KV Admission Slots"
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
    >
      <Axis label="slots">
        {nodeIDs.map(nid => (
          <>
            <Metric
              key={nid}
              name="cr.node.admission.granter.total_slots.kv"
              title={"Total Slots " + nodeDisplayName(nodeDisplayNameByID, nid)}
              sources={[nid]}
            />
            <Metric
              key={nid}
              name="cr.node.admission.granter.used_slots.kv"
              title={"Used Slots " + nodeDisplayName(nodeDisplayNameByID, nid)}
              sources={[nid]}
            />
          </>
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="KV Admission IO Tokens Exhausted Duration Per Second"
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
    >
      <Axis label="duration (micros/sec)">
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
      title="Flow Tokens Wait Time: 75th percentile"
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Duration} label="p75 flow token wait duration">
        {nodeIDs.map(nid => (
          <>
            <Metric
              key={nid}
              name="cr.node.kvadmission.flow_controller.regular_wait_duration-p75"
              title={
                "Regular flow token wait time " +
                nodeDisplayName(nodeDisplayNameByID, nid)
              }
              sources={[nid]}
              downsampleMax
            />
            <Metric
              key={nid}
              name="cr.node.kvadmission.flow_controller.elastic_wait_duration-p75"
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
      title="Requests Waiting For Flow Tokens"
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
    >
      <Axis label="Count">
        {nodeIDs.map(nid => (
          <>
            <Metric
              key={nid}
              name="cr.node.kvadmission.flow_controller.regular_requests_waiting"
              title={
                "Regular requests waiting " +
                nodeDisplayName(nodeDisplayNameByID, nid)
              }
              sources={[nid]}
            />
            <Metric
              key={nid}
              name="cr.node.kvadmission.flow_controller.elastic_requests_waiting"
              title={
                "Elastic requests waiting " +
                nodeDisplayName(nodeDisplayNameByID, nid)
              }
              sources={[nid]}
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
      title="Admission Work Rate"
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
    >
      <Axis label="work rate">
        {nodeIDs.map(nid => (
          <>
            <Metric
              key={nid}
              name="cr.node.admission.admitted.kv"
              title={
                "KV request rate " + nodeDisplayName(nodeDisplayNameByID, nid)
              }
              sources={[nid]}
              nonNegativeRate
            />
            <Metric
              key={nid}
              name="cr.node.admission.admitted.kv-stores"
              title={
                "KV write request rate " +
                nodeDisplayName(nodeDisplayNameByID, nid)
              }
              sources={[nid]}
              nonNegativeRate
            />
            <Metric
              key={nid}
              name="cr.node.admission.admitted.sql-kv-response"
              title={
                "SQL-KV response rate " +
                nodeDisplayName(nodeDisplayNameByID, nid)
              }
              sources={[nid]}
              nonNegativeRate
            />
            <Metric
              key={nid}
              name="cr.node.admission.admitted.sql-sql-response"
              title={
                "SQL-SQL response rate " +
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
      title="Admission Delay Rate"
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
    >
      <Axis label="delay rate (micros/sec)">
        {nodeIDs.map(nid => (
          <>
            <Metric
              key={nid}
              name="cr.node.admission.wait_durations.kv-sum"
              title={"KV " + nodeDisplayName(nodeDisplayNameByID, nid)}
              sources={[nid]}
              nonNegativeRate
            />
            <Metric
              key={nid}
              name="cr.node.admission.wait_durations.kv-stores-sum"
              title={"KV write " + nodeDisplayName(nodeDisplayNameByID, nid)}
              sources={[nid]}
              nonNegativeRate
            />
            <Metric
              key={nid}
              name="cr.node.admission.wait_durations.sql-kv-response-sum"
              title={
                "SQL-KV response " + nodeDisplayName(nodeDisplayNameByID, nid)
              }
              sources={[nid]}
              nonNegativeRate
            />
            <Metric
              key={nid}
              name="cr.node.admission.wait_durations.sql-sql-response-sum"
              title={
                "SQL-SQL response " + nodeDisplayName(nodeDisplayNameByID, nid)
              }
              sources={[nid]}
              nonNegativeRate
            />
          </>
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Admission Delay: 75th percentile"
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Duration} label="delay for requests that waited">
        {nodeIDs.map(nid => (
          <>
            <Metric
              key={nid}
              name="cr.node.admission.wait_durations.kv-p75"
              title={"KV " + nodeDisplayName(nodeDisplayNameByID, nid)}
              sources={[nid]}
              downsampleMax
            />
            <Metric
              key={nid}
              name="cr.node.admission.wait_durations.kv-stores-p75"
              title={"KV write " + nodeDisplayName(nodeDisplayNameByID, nid)}
              sources={[nid]}
              downsampleMax
            />
            <Metric
              key={nid}
              name="cr.node.admission.wait_durations.sql-kv-response-p75"
              title={
                "SQL-KV response " + nodeDisplayName(nodeDisplayNameByID, nid)
              }
              sources={[nid]}
              downsampleMax
            />
            <Metric
              key={nid}
              name="cr.node.admission.wait_durations.sql-sql-response-p75"
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
  ];
}
