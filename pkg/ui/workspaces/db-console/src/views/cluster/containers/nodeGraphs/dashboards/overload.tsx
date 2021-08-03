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
import _ from "lodash";

import { LineGraph } from "src/views/cluster/components/linegraph";
import {
  Metric,
  Axis,
  AxisUnits,
} from "src/views/shared/components/metricQuery";

import {
  GraphDashboardProps,
  nodeDisplayName,
  storeIDsForNode,
} from "./dashboardUtils";

export default function(props: GraphDashboardProps) {
  const { nodeIDs, nodesSummary, nodeSources, storeSources } = props;

  return [
    <LineGraph title="CPU Percent" sources={nodeSources}>
      <Axis units={AxisUnits.Percentage} label="CPU">
        {nodeIDs.map(nid => (
          <Metric
            name="cr.node.sys.cpu.combined.percent-normalized"
            title={nodeDisplayName(nodesSummary, nid)}
            sources={[nid]}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Runnable Goroutines per CPU"
      sources={nodeSources}
      tooltip={`The number of Goroutines waiting per CPU.`}
    >
      <Axis label="goroutines">
        {nodeIDs.map(nid => (
          <Metric
            name="cr.node.sys.runnable.goroutines.per.cpu"
            title={nodeDisplayName(nodesSummary, nid)}
            sources={[nid]}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="LSM L0 Health"
      sources={storeSources}
      tooltip={`The number of files and sublevels within Level 0.`}
    >
      <Axis label="count">
        {nodeIDs.map(nid => (
          <>
            <Metric
              key={nid}
              name="cr.store.storage.l0-sublevels"
              title={"L0 Sublevels " + nodeDisplayName(nodesSummary, nid)}
              sources={storeIDsForNode(nodesSummary, nid)}
            />
            <Metric
              key={nid}
              name="cr.store.storage.l0-num-files"
              title={"L0 Files " + nodeDisplayName(nodesSummary, nid)}
              sources={storeIDsForNode(nodesSummary, nid)}
            />
          </>
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph title="KV Admission Slots" sources={nodeSources}>
      <Axis label="slots">
        {nodeIDs.map(nid => (
          <>
            <Metric
              key={nid}
              name="cr.node.admission.granter.total_slots.kv"
              title="Total Slots"
              sources={[nid]}
            />
            <Metric
              key={nid}
              name="cr.node.admission.granter.used_slots.kv"
              title="Used Slots"
              sources={[nid]}
            />
          </>
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="KV Admission IO Tokens Exhausted Duration Per Second"
      sources={nodeSources}
    >
      <Axis label="duration (micros/sec)">
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
            name="cr.node.admission.granter.io_tokens_exhausted_duration.kv"
            title="IO Exhausted"
            sources={[nid]}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph title="Admission Work Rate" sources={nodeSources}>
      <Axis label="work rate">
        {nodeIDs.map(nid => (
          <>
            <Metric
              key={nid}
              name="cr.node.admission.admitted.kv"
              title="KV request rate"
              sources={[nid]}
              nonNegativeRate
            />
            <Metric
              key={nid}
              name="cr.node.admission.admitted.kv-stores"
              title="KV write request rate"
              sources={[nid]}
              nonNegativeRate
            />
            <Metric
              key={nid}
              name="cr.node.admission.admitted.sql-kv-response"
              title="SQL-KV response rate"
              sources={[nid]}
              nonNegativeRate
            />
            <Metric
              key={nid}
              name="cr.node.admission.admitted.sql-sql-response"
              title="SQL-SQL response rate"
              sources={[nid]}
              nonNegativeRate
            />
          </>
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph title="Admission Delay Rate" sources={nodeSources}>
      <Axis label="delay rate (micros/sec)">
        {nodeIDs.map(nid => (
          <>
            <Metric
              key={nid}
              name="cr.node.admission.wait_sum.kv"
              title="KV"
              sources={[nid]}
              nonNegativeRate
            />
            <Metric
              key={nid}
              name="cr.node.admission.wait_sum.kv-stores"
              title="KV write"
              sources={[nid]}
              nonNegativeRate
            />
            <Metric
              key={nid}
              name="cr.node.admission.wait_sum.sql-kv-response"
              title="SQL-KV response"
              sources={[nid]}
              nonNegativeRate
            />
            <Metric
              key={nid}
              name="cr.node.admission.wait_sum.sql-sql-response"
              title="SQL-SQL response"
              sources={[nid]}
              nonNegativeRate
            />
          </>
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph title="Admission Delay: 75th percentile" sources={nodeSources}>
      <Axis label="delay for requests that waited (nanos)">
        {nodeIDs.map(nid => (
          <>
            <Metric
              key={nid}
              name="cr.node.admission.wait_durations.kv-p75"
              title="KV"
              sources={[nid]}
              downsampleMax
            />
            <Metric
              key={nid}
              name="cr.node.admission.wait_durations.kv-stores-p75"
              title="KV write"
              sources={[nid]}
              downsampleMax
            />
            <Metric
              key={nid}
              name="cr.node.admission.wait_durations.sql-kv-response-p75"
              title="SQL-KV response"
              sources={[nid]}
              downsampleMax
            />
            <Metric
              key={nid}
              name="cr.node.admission.wait_durations.sql-sql-response-p75"
              title="SQL-SQL response"
              sources={[nid]}
              downsampleMax
            />
          </>
        ))}
      </Axis>
    </LineGraph>,
  ];
}
