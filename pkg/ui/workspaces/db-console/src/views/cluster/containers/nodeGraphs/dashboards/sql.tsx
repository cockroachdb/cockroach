// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { AxisUnits } from "@cockroachlabs/cluster-ui";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import map from "lodash/map";
import React from "react";

import LineGraph from "src/views/cluster/components/linegraph";
import {
  StatementDenialsClusterSettingsTooltip,
  TransactionRestartsToolTip,
} from "src/views/cluster/containers/nodeGraphs/dashboards/graphTooltips";
import { Metric, Axis } from "src/views/shared/components/metricQuery";

import { GraphDashboardProps, nodeDisplayName } from "./dashboardUtils";

import TimeSeriesQueryAggregator = cockroach.ts.tspb.TimeSeriesQueryAggregator;

export default function (props: GraphDashboardProps) {
  const {
    nodeIDs,
    nodeSources,
    tooltipSelection,
    nodeDisplayNameByID,
    tenantSource,
  } = props;

  return [
    <LineGraph
      title="Open SQL Sessions"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`The total number of open SQL Sessions ${tooltipSelection}.`}
      showMetricsInTooltip={true}
    >
      <Axis label="connections">
        {map(nodeIDs, node => (
          <Metric
            key={node}
            name="cr.node.sql.conns"
            title={nodeDisplayName(nodeDisplayNameByID, node)}
            sources={[node]}
            downsampleMax
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="SQL Connection Rate"
      isKvGraph={false}
      sources={nodeSources}
      tooltip={`Rate of SQL connection attempts ${tooltipSelection}`}
      showMetricsInTooltip={true}
    >
      <Axis label="connections per second">
        {map(nodeIDs, node => (
          <Metric
            key={node}
            name="cr.node.sql.new_conns"
            title={nodeDisplayName(nodeDisplayNameByID, node)}
            sources={[node]}
            downsampler={TimeSeriesQueryAggregator.MAX}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Upgrades of SQL Transaction Isolation Level"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`The total number of times a SQL transaction was upgraded to a stronger isolation level ${tooltipSelection}. If this metric is non-zero, then your application may be affected by the upcoming support of additional isolation levels.`}
      showMetricsInTooltip={true}
    >
      <Axis label="transactions">
        <Metric
          name="cr.node.sql.txn.upgraded_iso_level.count"
          title="Upgrades of Transaction Isolation Level"
          downsampleMax
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Open SQL Transactions"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`The total number of open SQL transactions ${tooltipSelection}.`}
      showMetricsInTooltip={true}
    >
      <Axis label="transactions">
        <Metric
          name="cr.node.sql.txns.open"
          title="Open Transactions"
          downsampleMax
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Active SQL Statements"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`The total number of running SQL statements ${tooltipSelection}.`}
      showMetricsInTooltip={true}
    >
      <Axis label="queries">
        <Metric
          name="cr.node.sql.statements.active"
          title="Active Statements"
          downsampleMax
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="SQL Byte Traffic"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`The total amount of SQL client network traffic in bytes per second ${tooltipSelection}.`}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Bytes} label="byte traffic">
        <Metric name="cr.node.sql.bytesin" title="Bytes In" nonNegativeRate />
        <Metric name="cr.node.sql.bytesout" title="Bytes Out" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="SQL Queries Per Second"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`A ten-second moving average of the # of SELECT, INSERT, UPDATE, and
          DELETE statements, and the sum of all four, successfully executed per second ${tooltipSelection}.`}
      showMetricsInTooltip={true}
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
      title="SQL Statement Errors"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`The number of statements which returned a planning, runtime, or
          client-side retry error.`}
      showMetricsInTooltip={true}
    >
      <Axis label="errors">
        <Metric
          name="cr.node.sql.failure.count"
          title="Errors"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="SQL Statement Contention"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`The total number of SQL statements that experienced contention ${tooltipSelection}.`}
      showMetricsInTooltip={true}
    >
      <Axis label="queries">
        <Metric
          name="cr.node.sql.distsql.contended_queries.count"
          title="Contention"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Full Table/Index Scans"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`The total number of full table/index scans per second ${tooltipSelection}.`}
      showMetricsInTooltip={true}
    >
      <Axis label="full scans per second">
        {map(nodeIDs, node => (
          <Metric
            key={node}
            name="cr.node.sql.full.scan.count"
            title={nodeDisplayName(nodeDisplayNameByID, node)}
            sources={[node]}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Transaction Deadlocks"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`The total number of transaction per second; typically, should be 0 ${tooltipSelection}.`}
      showMetricsInTooltip={true}
    >
      <Axis label="transaction deadlocks per second">
        {map(nodeIDs, node => (
          <Metric
            key={node}
            name="cr.store.txnwaitqueue.deadlocks_total"
            title={nodeDisplayName(nodeDisplayNameByID, node)}
            sources={[node]}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Active Flows for Distributed SQL Statements"
      isKvGraph={false}
      tenantSource={tenantSource}
      tooltip={`The number of flows on each node contributing to currently running
          distributed SQL statements.`}
      showMetricsInTooltip={true}
    >
      <Axis label="flows">
        {map(nodeIDs, node => (
          <Metric
            key={node}
            name="cr.node.sql.distsql.flows.active"
            title={nodeDisplayName(nodeDisplayNameByID, node)}
            sources={[node]}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Connection Latency: 99th percentile"
      isKvGraph={false}
      tenantSource={tenantSource}
      tooltip={`Over the last minute, this node established and authenticated 99% of
          connections within this time.`}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {map(nodeIDs, node => (
          <Metric
            key={node}
            name="cr.node.sql.conn.latency-p99"
            title={nodeDisplayName(nodeDisplayNameByID, node)}
            sources={[node]}
            downsampleMax
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Connection Latency: 90th percentile"
      isKvGraph={false}
      tenantSource={tenantSource}
      tooltip={`Over the last minute, this node established and authenticated 90% of
          connections within this time.`}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {map(nodeIDs, node => (
          <Metric
            key={node}
            name="cr.node.sql.conn.latency-p90"
            title={nodeDisplayName(nodeDisplayNameByID, node)}
            sources={[node]}
            downsampleMax
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Service Latency: SQL Statements, 99.99th percentile"
      isKvGraph={false}
      tenantSource={tenantSource}
      tooltip={
        <div>
          Over the last minute, this node executed 99.99% of SQL statements
          within this time.&nbsp;
          <em>
            This time only includes SELECT, INSERT, UPDATE and DELETE statements
            and does not include network latency between the node and client.
          </em>
        </div>
      }
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {map(nodeIDs, node => (
          <Metric
            key={node}
            name="cr.node.sql.service.latency-p99.99"
            title={nodeDisplayName(nodeDisplayNameByID, node)}
            sources={[node]}
            downsampleMax
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Service Latency: SQL Statements, 99.9th percentile"
      isKvGraph={false}
      tenantSource={tenantSource}
      tooltip={
        <div>
          Over the last minute, this node executed 99.9% of SQL statements
          within this time.&nbsp;
          <em>
            This time only includes SELECT, INSERT, UPDATE and DELETE statements
            and does not include network latency between the node and client.
          </em>
        </div>
      }
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {map(nodeIDs, node => (
          <Metric
            key={node}
            name="cr.node.sql.service.latency-p99.9"
            title={nodeDisplayName(nodeDisplayNameByID, node)}
            sources={[node]}
            downsampleMax
          />
        ))}
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
      title="Service Latency: SQL Statements, 90th percentile"
      isKvGraph={false}
      tenantSource={tenantSource}
      tooltip={
        <div>
          Over the last minute, this node executed 90% of SQL statements within
          this time.&nbsp;
          <em>
            This time only includes SELECT, INSERT, UPDATE and DELETE statements
            and does not include network latency between the node and client.
          </em>
        </div>
      }
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {map(nodeIDs, node => (
          <Metric
            key={node}
            name="cr.node.sql.service.latency-p90"
            title={nodeDisplayName(nodeDisplayNameByID, node)}
            sources={[node]}
            downsampleMax
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="KV Execution Latency: 99th percentile"
      isKvGraph={true}
      tenantSource={tenantSource}
      tooltip={`The 99th percentile of latency between query requests and responses
          over a 1 minute period. Values are displayed individually for each
          node.`}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {map(nodeIDs, node => (
          <Metric
            key={node}
            name="cr.node.exec.latency-p99"
            title={nodeDisplayName(nodeDisplayNameByID, node)}
            sources={[node]}
            downsampleMax
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="KV Execution Latency: 90th percentile"
      isKvGraph={true}
      tenantSource={tenantSource}
      tooltip={`The 90th percentile of latency between query requests and responses
          over a 1 minute period. Values are displayed individually for each
          node.`}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {map(nodeIDs, node => (
          <Metric
            key={node}
            name="cr.node.exec.latency-p90"
            title={nodeDisplayName(nodeDisplayNameByID, node)}
            sources={[node]}
            downsampleMax
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Transactions"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`The total number of transactions initiated, committed, rolled back, or
          aborted per second ${tooltipSelection}.`}
      showMetricsInTooltip={true}
    >
      <Axis label="transactions">
        <Metric
          name="cr.node.sql.txn.begin.count"
          title="Begin"
          nonNegativeRate
        />
        <Metric
          name="cr.node.sql.txn.commit.count"
          title="Commits"
          nonNegativeRate
        />
        <Metric
          name="cr.node.sql.txn.rollback.count"
          title="Rollbacks"
          nonNegativeRate
        />
        <Metric
          name="cr.node.sql.txn.abort.count"
          title="Aborts"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Transaction Restarts"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={
        <TransactionRestartsToolTip tooltipSelection={tooltipSelection} />
      }
      showMetricsInTooltip={true}
    >
      <Axis label="restarts">
        <Metric
          name="cr.node.txn.restarts.writetooold"
          title="Write Too Old"
          nonNegativeRate
        />
        <Metric
          name="cr.node.txn.restarts.serializable"
          title="Forwarded Timestamp (iso=serializable)"
          nonNegativeRate
        />
        <Metric
          name="cr.node.txn.restarts.asyncwritefailure"
          title="Async Consensus Failure"
          nonNegativeRate
        />
        <Metric
          name="cr.node.txn.restarts.readwithinuncertainty"
          title="Read Within Uncertainty Interval"
          nonNegativeRate
        />
        <Metric
          name="cr.node.txn.restarts.txnaborted"
          title="Aborted"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Transaction Latency: 99th percentile"
      isKvGraph={false}
      tenantSource={tenantSource}
      tooltip={
        <div>
          Over the last minute, this node executed 99% of transactions within
          this time.
          <em>
            This time does not include network latency between the node and
            client.
          </em>
        </div>
      }
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {map(nodeIDs, node => (
          <Metric
            key={node}
            name="cr.node.sql.txn.latency-p99"
            title={nodeDisplayName(nodeDisplayNameByID, node)}
            sources={[node]}
            downsampleMax
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Transaction Latency: 90th percentile"
      isKvGraph={false}
      tenantSource={tenantSource}
      tooltip={
        <div>
          Over the last minute, this node executed 90% of transactions within
          this time.
          <em>
            This time does not include network latency between the node and
            client.
          </em>
        </div>
      }
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {map(nodeIDs, node => (
          <Metric
            key={node}
            name="cr.node.sql.txn.latency-p90"
            title={nodeDisplayName(nodeDisplayNameByID, node)}
            sources={[node]}
            downsampleMax
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="SQL Memory"
      isKvGraph={false}
      tenantSource={tenantSource}
      tooltip={`The current amount of allocated SQL memory. This amount is compared
          against the node's --max-sql-memory flag.`}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Bytes} label="allocated bytes">
        {map(nodeIDs, node => (
          <Metric
            key={node}
            name="cr.node.sql.mem.root.current"
            title={nodeDisplayName(nodeDisplayNameByID, node)}
            sources={[node]}
            downsampleMax
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Schema Changes"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`The total number of DDL statements per second ${tooltipSelection}.`}
      showMetricsInTooltip={true}
    >
      <Axis label="statements">
        <Metric
          name="cr.node.sql.ddl.count"
          title="DDL Statements"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Statement Denials: Cluster Settings"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={
        <StatementDenialsClusterSettingsTooltip
          tooltipSelection={tooltipSelection}
        />
      }
      showMetricsInTooltip={true}
    >
      <Axis label="statements">
        <Metric
          name="cr.node.sql.feature_flag_denial"
          title="Statements Denied"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Distributed Query Error Reruns"
      isKvGraph={false}
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`The total number of times the distributed query errors were rerun as
          local ${tooltipSelection}.`}
      showMetricsInTooltip={true}
    >
      <Axis label="queries">
        <Metric
          name="cr.node.sql.distsql.dist_query_rerun_locally.count"
          title="Reruns"
          downsampleMax
        />
        <Metric
          name="cr.node.sql.distsql.dist_query_rerun_locally.failure_count"
          title="Failures"
          downsampleMax
        />
      </Axis>
    </LineGraph>,
  ];
}
