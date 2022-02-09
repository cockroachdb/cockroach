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

import { GraphDashboardProps, nodeDisplayName } from "./dashboardUtils";
import {
  StatementDenialsClusterSettingsTooltip,
  TransactionRestartsToolTip,
} from "src/views/cluster/containers/nodeGraphs/dashboards/graphTooltips";

export default function(props: GraphDashboardProps) {
  const { nodeIDs, nodesSummary, nodeSources, tooltipSelection } = props;

  return [
    <LineGraph
      title="Open SQL Sessions"
      sources={nodeSources}
      tooltip={`The total number of open SQL Sessions ${tooltipSelection}.`}
    >
      <Axis label="connections">
        {_.map(nodeIDs, node => (
          <Metric
            key={node}
            name="cr.node.sql.conns"
            title={nodeDisplayName(nodesSummary, node)}
            sources={[node]}
            downsampleMax
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Open SQL Transactions"
      sources={nodeSources}
      tooltip={`The total number of open SQL transactions  ${tooltipSelection}.`}
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
      sources={nodeSources}
      tooltip={`The total number of running SQL statements ${tooltipSelection}.`}
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
      sources={nodeSources}
      tooltip={`The total amount of SQL client network traffic in bytes per second ${tooltipSelection}.`}
    >
      <Axis units={AxisUnits.Bytes} label="byte traffic">
        <Metric name="cr.node.sql.bytesin" title="Bytes In" nonNegativeRate />
        <Metric name="cr.node.sql.bytesout" title="Bytes Out" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="SQL Statements"
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
      title="SQL Statement Errors"
      sources={nodeSources}
      tooltip={
        "The number of statements which returned a planning or runtime error."
      }
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
      sources={nodeSources}
      tooltip={`The total number of SQL statements that experienced contention ${tooltipSelection}.`}
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
      sources={nodeSources}
      tooltip={`The total number of full table/index scans ${tooltipSelection}.`}
    >
      <Axis label="full scans">
        {_.map(nodeIDs, node => (
          <Metric
            key={node}
            name="cr.node.sql.full.scan.count"
            title={nodeDisplayName(nodesSummary, node)}
            sources={[node]}
            downsampleMax
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Active Flows for Distributed SQL Statements"
      tooltip="The number of flows on each node contributing to currently running distributed SQL statements."
    >
      <Axis label="flows">
        {_.map(nodeIDs, node => (
          <Metric
            key={node}
            name="cr.node.sql.distsql.flows.active"
            title={nodeDisplayName(nodesSummary, node)}
            sources={[node]}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Connection Latency: 99th percentile"
      tooltip={`Over the last minute, this node established and authenticated 99% of connections within this time.`}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {_.map(nodeIDs, node => (
          <Metric
            key={node}
            name="cr.node.sql.conn.latency-p99"
            title={nodeDisplayName(nodesSummary, node)}
            sources={[node]}
            downsampleMax
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Connection Latency: 90th percentile"
      tooltip={`Over the last minute, this node established and authenticated 90% of connections within this time.`}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {_.map(nodeIDs, node => (
          <Metric
            key={node}
            name="cr.node.sql.conn.latency-p90"
            title={nodeDisplayName(nodesSummary, node)}
            sources={[node]}
            downsampleMax
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Service Latency: SQL Statements, 99th percentile"
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
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {_.map(nodeIDs, node => (
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

    <LineGraph
      title="Service Latency: SQL Statements, 90th percentile"
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
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {_.map(nodeIDs, node => (
          <Metric
            key={node}
            name="cr.node.sql.service.latency-p90"
            title={nodeDisplayName(nodesSummary, node)}
            sources={[node]}
            downsampleMax
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="KV Execution Latency: 99th percentile"
      tooltip={`The 99th percentile of latency between query requests and responses over a
          1 minute period. Values are displayed individually for each node.`}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {_.map(nodeIDs, node => (
          <Metric
            key={node}
            name="cr.node.exec.latency-p99"
            title={nodeDisplayName(nodesSummary, node)}
            sources={[node]}
            downsampleMax
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="KV Execution Latency: 90th percentile"
      tooltip={`The 90th percentile of latency between query requests and responses over a
           1 minute period. Values are displayed individually for each node.`}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {_.map(nodeIDs, node => (
          <Metric
            key={node}
            name="cr.node.exec.latency-p90"
            title={nodeDisplayName(nodesSummary, node)}
            sources={[node]}
            downsampleMax
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Transactions"
      sources={nodeSources}
      tooltip={`The total number of transactions initiated, committed, rolled back,
           or aborted per second ${tooltipSelection}.`}
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
      sources={nodeSources}
      tooltip={
        <TransactionRestartsToolTip tooltipSelection={tooltipSelection} />
      }
    >
      <Axis label="restarts">
        <Metric
          name="cr.node.txn.restarts.writetooold"
          title="Write Too Old"
          nonNegativeRate
        />
        <Metric
          name="cr.node.txn.restarts.writetoooldmulti"
          title="Write Too Old (multiple)"
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
        <Metric
          name="cr.node.txn.restarts.txnpush"
          title="Push Failure"
          nonNegativeRate
        />
        <Metric
          name="cr.node.txn.restarts.unknown"
          title="Unknown"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Transaction Latency: 99th percentile"
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
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {_.map(nodeIDs, node => (
          <Metric
            key={node}
            name="cr.node.sql.txn.latency-p99"
            title={nodeDisplayName(nodesSummary, node)}
            sources={[node]}
            downsampleMax
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Transaction Latency: 90th percentile"
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
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {_.map(nodeIDs, node => (
          <Metric
            key={node}
            name="cr.node.sql.txn.latency-p90"
            title={nodeDisplayName(nodesSummary, node)}
            sources={[node]}
            downsampleMax
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="SQL Memory"
      tooltip={`The current amount of allocated SQL memory. This amount is
         compared against the node's --max-sql-memory flag.`}
    >
      <Axis units={AxisUnits.Bytes} label="allocated bytes">
        {_.map(nodeIDs, node => (
          <Metric
            key={node}
            name="cr.node.sql.mem.root.current"
            title={nodeDisplayName(nodesSummary, node)}
            sources={[node]}
            downsampleMax
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Schema Changes"
      sources={nodeSources}
      tooltip={`The total number of DDL statements per second ${tooltipSelection}.`}
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
      sources={nodeSources}
      tooltip={
        <StatementDenialsClusterSettingsTooltip
          tooltipSelection={tooltipSelection}
        />
      }
    >
      <Axis label="statements">
        <Metric
          name="cr.node.sql.feature_flag_denial"
          title="Statements Denied"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,
  ];
}
