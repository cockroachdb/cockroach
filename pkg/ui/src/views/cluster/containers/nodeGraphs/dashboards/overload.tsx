// Copyright 2018 The Cockroach Authors.
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

export default function (props: GraphDashboardProps) {
  const {
    nodeIDs,
    nodesSummary,
    nodeSources,
    storeSources,
    tooltipSelection,
  } = props;

  return [
    <LineGraph
      title="SQL Queries"
      sources={nodeSources}
      tooltip={`A ten-second moving average of the # of SELECT, INSERT, UPDATE, and DELETE statements
        successfully executed per second ${tooltipSelection}.`}
    >
      <Axis label="queries">
        <Metric
          name="cr.node.sql.select.count"
          title="Selects"
          nonNegativeRate
        />
        <Metric
          name="cr.node.sql.update.count"
          title="Updates"
          nonNegativeRate
        />
        <Metric
          name="cr.node.sql.insert.count"
          title="Inserts"
          nonNegativeRate
        />
        <Metric
          name="cr.node.sql.delete.count"
          title="Deletes"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Service Latency: SQL, 99th percentile"
      tooltip={
        <div>
          Over the last minute, this node executed 99% of queries within this
          time.&nbsp;
          <em>
            This time does not include network latency between the node and
            client.
          </em>
        </div>
      }
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {_.map(nodeIDs, (node) => (
          <Metric
            key={node}
            name="cr.node.sql.service.latency-p99"
            title={nodeDisplayName(nodesSummary, node)}
            sources={[node]}
            downsampleMax
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph title="CPU Percent" sources={nodeSources}>
      <Axis units={AxisUnits.Percentage} label="CPU">
        {nodeIDs.map((nid) => (
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
        {nodeIDs.map((nid) => (
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
        {nodeIDs.map((nid) => (
          <>
            <Metric
              key={nid}
              name="cr.store.rocksdb.l0-sublevels"
              title={"L0 Sublevels " + nodeDisplayName(nodesSummary, nid)}
              sources={storeIDsForNode(nodesSummary, nid)}
            />
            <Metric
              key={nid}
              name="cr.store.rocksdb.l0-num-files"
              title={"L0 Files " + nodeDisplayName(nodesSummary, nid)}
              sources={storeIDsForNode(nodesSummary, nid)}
            />
          </>
        ))}
      </Axis>
    </LineGraph>,
  ];
}
