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
      title="LSM L0 Health"
      sources={storeSources}
      tenantSource={tenantSource}
      tooltip={`The number of files and sublevels within Level 0.`}
    >
      <Axis label="count">
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
            <Metric
              key={nid}
              name="cr.store.storage.l0-num-files"
              title={"L0 Files " + nodeDisplayName(nodeDisplayNameByID, nid)}
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
      title="Admission Work Rate"
      sources={nodeSources}
      tenantSource={tenantSource}
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
    >
      <Axis label="delay rate (micros/sec)">
        {nodeIDs.map(nid => (
          <>
            <Metric
              key={nid}
              name="cr.node.admission.wait_sum.kv"
              title={"KV " + nodeDisplayName(nodeDisplayNameByID, nid)}
              sources={[nid]}
              nonNegativeRate
            />
            <Metric
              key={nid}
              name="cr.node.admission.wait_sum.kv-stores"
              title={"KV write " + nodeDisplayName(nodeDisplayNameByID, nid)}
              sources={[nid]}
              nonNegativeRate
            />
            <Metric
              key={nid}
              name="cr.node.admission.wait_sum.sql-kv-response"
              title={
                "SQL-KV response " + nodeDisplayName(nodeDisplayNameByID, nid)
              }
              sources={[nid]}
              nonNegativeRate
            />
            <Metric
              key={nid}
              name="cr.node.admission.wait_sum.sql-sql-response"
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
