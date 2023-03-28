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

import LineGraph from "src/views/cluster/components/linegraph";
import { Axis, Metric } from "src/views/shared/components/metricQuery";

import {
  GraphDashboardProps,
  nodeDisplayName,
  storeIDsForNode,
} from "./dashboardUtils";
import { CapacityGraphTooltip } from "src/views/cluster/containers/nodeGraphs/dashboards/graphTooltips";
import { AxisUnits } from "@cockroachlabs/cluster-ui";

export default function (props: GraphDashboardProps) {
  const {
    nodeIDs,
    nodeSources,
    storeSources,
    tooltipSelection,
    nodeDisplayNameByID,
    storeIDsByNodeID,
  } = props;

  return [
    <LineGraph
      title="SQL Statements"
      isKvGraph={false}
      sources={nodeSources}
      tooltip={`A moving average of the number of SELECT, INSERT, UPDATE, and DELETE statements
        successfully executed per second ${tooltipSelection}.`}
      preCalcGraphSize={true}
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
      title="Service Latency: SQL Statements, 99th percentile"
      isKvGraph={false}
      tooltip={
        <div>
          Over the last minute, this node executed 99% of SQL statements within
          this time.&nbsp;
          <em>
            This time only includes SELECT, INSERT, UPDATE and DELETE statements
            and does not include network latency between the node and client.
          </em>
        </div>
      }
      preCalcGraphSize={true}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {_.map(nodeIDs, node => (
          <Metric
            key={node}
            name="cr.node.sql.service.latency-p99"
            title={nodeDisplayName(nodeDisplayNameByID, node)}
            sources={[node]}
            downsampleMax
          />
        ))}
      </Axis>
    </LineGraph>,
    <LineGraph
      title="SQL Statement Contention"
      isKvGraph={false}
      sources={nodeSources}
      tooltip={`A moving average of the number of SQL statements executed per second that experienced contention ${tooltipSelection}.`}
      preCalcGraphSize={true}
    >
      <Axis label="Average number of queries per second">
        <Metric
          name="cr.node.sql.distsql.contended_queries.count"
          title="Contention"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Replicas per Node"
      tooltip={
        <div>
          The number of range replicas stored on this node.{" "}
          <em>
            Ranges are subsets of your data, which are replicated to ensure
            survivability.
          </em>
        </div>
      }
      preCalcGraphSize={true}
    >
      <Axis label="replicas">
        {_.map(nodeIDs, nid => (
          <Metric
            key={nid}
            name="cr.store.replicas"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={storeIDsForNode(storeIDsByNodeID, nid)}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Capacity"
      isKvGraph={true}
      sources={storeSources}
      tooltip={<CapacityGraphTooltip tooltipSelection={tooltipSelection} />}
      preCalcGraphSize={true}
    >
      <Axis units={AxisUnits.Bytes} label="capacity">
        <Metric name="cr.store.capacity" title="Max" />
        <Metric name="cr.store.capacity.available" title="Available" />
        <Metric name="cr.store.capacity.used" title="Used" />
      </Axis>
    </LineGraph>,
  ];
}
