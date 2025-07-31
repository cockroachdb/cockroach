// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { AxisUnits } from "@cockroachlabs/cluster-ui";
import map from "lodash/map";
import React from "react";

import LineGraph from "src/views/cluster/components/linegraph";
import { CapacityGraphTooltip } from "src/views/cluster/containers/nodeGraphs/dashboards/graphTooltips";
import { Axis, Metric } from "src/views/shared/components/metricQuery";

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
    tooltipSelection,
    nodeDisplayNameByID,
    storeIDsByNodeID,
    tenantSource,
  } = props;

  return [
    <LineGraph
      title="SQL Queries Per Second"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`A moving average of the number of SELECT, INSERT, UPDATE, and DELETE
          statements, and the sum of all four, successfully executed per second ${tooltipSelection}.`}
      showMetricsInTooltip={true}
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
        <Metric
          name="cr.node.sql.crud_query.count"
          title="Total Queries"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Service Latency: SQL Statements, 99th percentile"
      isKvGraph={false}
      tenantSource={tenantSource}
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
      showMetricsInTooltip={true}
      preCalcGraphSize={true}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {map(nodeIDs, node => (
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
      tenantSource={tenantSource}
      tooltip={`A moving average of the number of SQL statements executed per second
          that experienced contention ${tooltipSelection}.`}
      showMetricsInTooltip={true}
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
      tenantSource={tenantSource}
      isKvGraph={true}
      tooltip={
        <div>
          The number of range replicas stored on this node.{" "}
          <em>
            Ranges are subsets of your data, which are replicated to ensure
            survivability.
          </em>
        </div>
      }
      showMetricsInTooltip={true}
      preCalcGraphSize={true}
    >
      <Axis label="replicas">
        {map(nodeIDs, nid => (
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
      tenantSource={tenantSource}
      tooltip={<CapacityGraphTooltip tooltipSelection={tooltipSelection} />}
      showMetricsInTooltip={true}
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
