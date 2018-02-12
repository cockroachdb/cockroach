import React from "react";
import _ from "lodash";

import { LineGraph } from "src/views/cluster/components/linegraph";
import { Metric, Axis, AxisUnits } from "src/views/shared/components/metricQuery";

import { GraphDashboardProps, nodeDisplayName } from "./dashboardUtils";

export default function (props: GraphDashboardProps) {
  const { nodeIDs, nodesSummary, nodeSources, tooltipSelection } = props;

  return [
    <LineGraph
      title="SQL Connections"
      sources={nodeSources}
      tooltip={`The total number of active SQL connections ${tooltipSelection}.`}
    >
      <Axis label="connections">
        <Metric name="cr.node.sql.conns" title="Client Connections" />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="SQL Byte Traffic"
      sources={nodeSources}
      tooltip={
        `The total amount of SQL client network traffic in bytes per second ${tooltipSelection}.`
      }
    >
      <Axis units={AxisUnits.Bytes} label="byte traffic">
        <Metric name="cr.node.sql.bytesin" title="Bytes In" nonNegativeRate />
        <Metric name="cr.node.sql.bytesout" title="Bytes Out" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="SQL Queries"
      sources={nodeSources}
      tooltip={
        `A ten-second moving average of the # of SELECT, INSERT, UPDATE, and DELETE operations
        started per second ${tooltipSelection}.`
      }
    >
      <Axis label="queries">
        <Metric name="cr.node.sql.select.count" title="Total Reads" nonNegativeRate />
        <Metric name="cr.node.sql.distsql.select.count" title="DistSQL Reads" nonNegativeRate />
        <Metric name="cr.node.sql.update.count" title="Updates" nonNegativeRate />
        <Metric name="cr.node.sql.insert.count" title="Inserts" nonNegativeRate />
        <Metric name="cr.node.sql.delete.count" title="Deletes" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Active Distributed SQL Queries"
      sources={nodeSources}
      tooltip={`The total number of distributed SQL queries currently running ${tooltipSelection}.`}
    >
      <Axis label="queries">
        <Metric name="cr.node.sql.distsql.queries.active" title="Active Queries" />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Active Flows for Distributed SQL Queries"
      tooltip="The number of flows on each node contributing to currently running distributed SQL queries."
    >
      <Axis label="flows">
        {
          _.map(nodeIDs, (node) => (
            <Metric
              key={node}
              name="cr.node.sql.distsql.flows.active"
              title={nodeDisplayName(nodesSummary, node)}
              sources={[node]}
            />
          ))
        }
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Service Latency: SQL, 99th percentile"
      tooltip={(
        <div>
          Over the last minute, this node executed 99% of queries within this time.
          {" "}
          <em>This time does not include network latency between the node and client.</em>
        </div>
      )}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {
          _.map(nodeIDs, (node) => (
            <Metric
              key={node}
              name="cr.node.sql.service.latency-p99"
              title={nodeDisplayName(nodesSummary, node)}
              sources={[node]}
              downsampleMax
            />
          ))
        }
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Service Latency: SQL, 90th percentile"
      tooltip={(
        <div>
          Over the last minute, this node executed 90% of queries within this time.
          {" "}
          <em>This time does not include network latency between the node and client.</em>
        </div>
      )}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {
          _.map(nodeIDs, (node) => (
            <Metric
              key={node}
              name="cr.node.sql.service.latency-p90"
              title={nodeDisplayName(nodesSummary, node)}
              sources={[node]}
              downsampleMax
            />
          ))
        }
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Service Latency: DistSQL, 99th percentile"
      tooltip={
        `The latency of distributed SQL statements serviced over
           10 second periods ${tooltipSelection}.`
      }
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {
          _.map(nodeIDs, (node) => (
            <Metric
              key={node}
              name="cr.node.sql.distsql.service.latency-p99"
              title={nodeDisplayName(nodesSummary, node)}
              sources={[node]}
              downsampleMax
            />
          ))
        }
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Service Latency: DistSQL, 90th percentile"
      tooltip={
        `The latency of distributed SQL statements serviced over
           10 second periods ${tooltipSelection}.`
      }
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {
          _.map(nodeIDs, (node) => (
            <Metric
              key={node}
              name="cr.node.sql.distsql.service.latency-p90"
              title={nodeDisplayName(nodesSummary, node)}
              sources={[node]}
              downsampleMax
            />
          ))
        }
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Execution Latency: 99th percentile"
      tooltip={
        `The 99th percentile of latency between query requests and responses over a
          1 minute period. Values are displayed individually for each node.`
      }
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {
          _.map(nodeIDs, (node) => (
            <Metric
              key={node}
              name="cr.node.exec.latency-p99"
              title={nodeDisplayName(nodesSummary, node)}
              sources={[node]}
              downsampleMax
            />
          ))
        }
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Execution Latency: 90th percentile"
      tooltip={
        `The 90th percentile of latency between query requests and responses over a
           1 minute period. Values are displayed individually for each node.`
      }
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {
          _.map(nodeIDs, (node) => (
            <Metric
              key={node}
              name="cr.node.exec.latency-p90"
              title={nodeDisplayName(nodesSummary, node)}
              sources={[node]}
              downsampleMax
            />
          ))
        }
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Transactions"
      sources={nodeSources}
      tooltip={
        `The total number of transactions opened, committed, rolled back,
           or aborted per second ${tooltipSelection}.`
      }
    >
      <Axis label="transactions">
        <Metric name="cr.node.sql.txn.begin.count" title="Begin" nonNegativeRate />
        <Metric name="cr.node.sql.txn.commit.count" title="Commits" nonNegativeRate />
        <Metric name="cr.node.sql.txn.rollback.count" title="Rollbacks" nonNegativeRate />
        <Metric name="cr.node.sql.txn.abort.count" title="Aborts" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Schema Changes"
      sources={nodeSources}
      tooltip={`The total number of DDL statements per second ${tooltipSelection}.`}
    >
      <Axis label="statements">
        <Metric name="cr.node.sql.ddl.count" title="DDL Statements" nonNegativeRate />
      </Axis>
    </LineGraph>,
  ];
}
