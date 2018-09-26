import React from "react";
import _ from "lodash";

import { AggregationLevel } from "src/redux/aggregationLevel";
import { LineGraph } from "src/views/cluster/components/linegraph";
import { Metric, Axis, AxisUnits } from "src/views/shared/components/metricQuery";

import { GraphDashboardProps, nodeDisplayName } from "./dashboardUtils";

export default function (props: GraphDashboardProps) {
  const { nodeIDs, nodesSummary, nodeSources, tooltipSelection, aggregationLevel } = props;

  function default_aggregate(name: string, title: string, otherProps?: { [key: string]: boolean }) {
    if (aggregationLevel === AggregationLevel.Cluster) {
      return (
        <Metric name={name} title={title} sources={nodeSources} {...otherProps} />
      );
    }

    return nodeIDs.map((nid) => (
      <Metric name={name} title={nodeDisplayName(nodesSummary, nid)} sources={[nid]} {...otherProps} />
    ));
  }

  const charts = [];

  charts.push(
    <LineGraph
      title="SQL Connections"
      tooltip={`The total number of active SQL connections ${tooltipSelection}.`}
    >
      <Axis label="connections">
        {default_aggregate("cr.node.sql.conns", "Client Connections")}
      </Axis>
    </LineGraph>,
  );

  if (aggregationLevel === AggregationLevel.Cluster) {
    charts.push(
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
    );
  } else {
    charts.push(
      <LineGraph
        title="SQL Byte Traffic"
        subtitle="In"
        sources={nodeSources}
        tooltip={
          `The total amount of SQL client network traffic in bytes per second ${tooltipSelection}.`
        }
      >
        <Axis units={AxisUnits.Bytes} label="byte traffic">
          {nodeIDs.map((nid) => (
            <Metric
              name="cr.node.sql.bytesin"
              title={nodeDisplayName(nodesSummary, nid)}
              nonNegativeRate
            />
          ))}
        </Axis>
      </LineGraph>,
    );
    charts.push(
      <LineGraph
        title="SQL Byte Traffic"
        subtitle="Out"
        sources={nodeSources}
        tooltip={
          `The total amount of SQL client network traffic in bytes per second ${tooltipSelection}.`
        }
      >
        <Axis units={AxisUnits.Bytes} label="byte traffic">
          {nodeIDs.map((nid) => (
            <Metric
              name="cr.node.sql.bytesout"
              title={nodeDisplayName(nodesSummary, nid)}
              nonNegativeRate
            />
          ))}
        </Axis>
      </LineGraph>,
    );
  }

  if (props.aggregationLevel === AggregationLevel.Cluster) {
    charts.push(
      <LineGraph
        title="Statements"
        sources={nodeSources}
        tooltip={
          `A ten-second moving average of the count of SELECT, INSERT, UPDATE, and DELETE statements
          started per second ${tooltipSelection}.`
        }
      >
        <Axis label="statements">
          <Metric name="cr.node.sql.select.count" title="Selects" nonNegativeRate />
          <Metric name="cr.node.sql.update.count" title="Updates" nonNegativeRate />
          <Metric name="cr.node.sql.insert.count" title="Inserts" nonNegativeRate />
          <Metric name="cr.node.sql.delete.count" title="Deletes" nonNegativeRate />
        </Axis>
      </LineGraph>,
    );
  } else {
    charts.push(
      <LineGraph
        title="Statements"
        tooltip={
          `A ten-second moving average of the count of SQL statements
          started per second ${tooltipSelection}.`
        }
      >
        <Axis label="statements">
          {
            _.map(nodeIDs, (node) => (
              <Metric
                key={node}
                name="cr.node.sql.query.count"
                title={nodeDisplayName(nodesSummary, node)}
                sources={[node]}
                nonNegativeRate
              />
            ))
          }
        </Axis>
      </LineGraph>,
    );
  }

  charts.push(
    <LineGraph
      title="SQL Query Errors"
      sources={nodeSources}
      tooltip={"The number of statements which returned a planning or runtime error."}
    >
      <Axis label="errors">
        {default_aggregate("cr.node.sql.failure.count", "Errors", { nonNegativeRate: true })}
      </Axis>
    </LineGraph>,
  );

  charts.push(
    <LineGraph
      title="Active Distributed SQL Queries"
      sources={nodeSources}
      tooltip={`The total number of distributed SQL queries currently running ${tooltipSelection}.`}
    >
      <Axis label="queries">
        {default_aggregate("cr.node.sql.distsql.queries.active", "Active Queries")}
      </Axis>
    </LineGraph>,
  );

  charts.push(
    <LineGraph
      title="Active Flows for Distributed SQL Queries"
      tooltip="The number of flows on each node contributing to currently running distributed SQL queries."
    >
      <Axis label="flows">
        {default_aggregate("cr.node.sql.distsql.flows.active", "Flows")}
      </Axis>
    </LineGraph>,
  );

  charts.push(
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
  );

  charts.push(
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
  );

  charts.push(
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
  );

  charts.push(
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
  );

  charts.push(
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
  );

  charts.push(
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
  );

  charts.push(
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
  );

  charts.push(
    <LineGraph
      title="Schema Changes"
      sources={nodeSources}
      tooltip={`The total number of DDL statements per second ${tooltipSelection}.`}
    >
      <Axis label="statements">
        {default_aggregate("cr.node.sql.ddl.count", "DDL Statements", { nonNegativeRate: true })}
      </Axis>
    </LineGraph>,
  );

  return charts;
}
