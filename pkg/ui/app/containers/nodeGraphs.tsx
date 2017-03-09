import _ from "lodash";
import * as React from "react";
import * as d3 from "d3";
import { IInjectedProps, Link } from "react-router";
import { connect } from "react-redux";
import { createSelector } from "reselect";

import {
  nodeIDAttr, dashboardNameAttr,
} from "../util/constants";

import { AdminUIState } from "../redux/state";
import { refreshNodes } from "../redux/apiReducers";
import GraphGroup from "../components/graphGroup";
import { SummaryBar, SummaryLabel, SummaryStat, SummaryMetricStat } from "../components/summaryBar";
import { Axis, AxisUnits } from "../components/graphs";
import { LineGraph } from "../components/linegraph";
import { Metric } from "../components/metric";
import { EventBox } from "../containers/events";
import { Bytes } from "../util/format";
import { NanoToMilli } from "../util/convert";
import { NodeStatus, MetricConstants, BytesUsed } from "../util/proto";

interface NodeGraphsOwnProps {
  refreshNodes: typeof refreshNodes;
  nodesQueryValid: boolean;
  nodeIds: string[];
  nodeStatusByID: {[s: string]: NodeStatus};
  nodeCount: number;
  capacityAvailable: number;
  capacityTotal: number;
  unavailableRanges: number;
}

type NodeGraphsProps = NodeGraphsOwnProps & IInjectedProps;

/**
 * Renders the main content of the help us page.
 */
class NodeGraphs extends React.Component<NodeGraphsProps, {}> {
  static displayTimeScale = true;

  refresh(props = this.props) {
    if (!props.nodesQueryValid) {
      props.refreshNodes();
    }
  }

  componentWillMount() {
    this.refresh();
  }

  componentWillReceiveProps(props: NodeGraphsProps) {
    this.refresh(props);
  }

  nodeAddress(nid: string) {
    let ns = this.props.nodeStatusByID[nid];
    if (!ns) {
      // This should only happen immediately after loading a page, and
      // associated graphs should display no data.
      return "unknown address";
    }
    return this.props.nodeStatusByID[nid].desc.address.address_field;
  }

  storeIDsForNode(nid: string): string[] {
    let ns = this.props.nodeStatusByID[nid];
    if (!ns) {
      return [];
    }
    return _.map(ns.store_statuses, (ss) => ss.desc.store_id.toString());
  }

  render() {
    let selectedNode = this.props.params[nodeIDAttr];
    let nodeSources =  (_.isString(selectedNode) && selectedNode !== "") ? [selectedNode] : null;
    let storeSources: string[] = null;

    // When "all" is the selected source, some graphs display a line for every
    // node in the cluster using the nodeIds collection. However, if a specific
    // node is already selected, these per-node graphs should only display data
    // only for the selected node.
    //
    // Similarly, if a single node is selected, we also need to restrict the
    // set of stores queried (only stores that belong to that node will be
    // queried).
    let nodeIds = this.props.nodeIds;
    if (nodeSources && nodeSources.length !== 0) {
      nodeIds = nodeSources;
      storeSources = [];
      _.each(nodeSources, (nid) => {
        _.each(this.storeIDsForNode(nid), (sid) => storeSources.push(sid));
      });
    }

    let dashboard = this.props.params[dashboardNameAttr];
    let specifier = (nodeSources && nodeSources.length === 1) ? `on node ${nodeSources[0]}` : "across all nodes";

    // Capacity math used in the summary status section.
    let { capacityTotal, capacityAvailable } = this.props;
    let capacityUsed = capacityTotal - capacityAvailable;
    let capacityPercent = capacityTotal !== 0 ? (capacityUsed / capacityTotal * 100) : 100;

    return <div className="section l-columns">
      <div className="chart-group l-columns__left">
        <GraphGroup groupId="node.overview" hide={dashboard !== "overview"}>
          <LineGraph title="SQL Queries" sources={nodeSources} tooltip={`The average number of SELECT, INSERT, UPDATE, and DELETE statements per second across ${specifier}.`}>
            <Axis>
              <Metric name="cr.node.sql.select.count" title="Total Reads" nonNegativeRate />
              <Metric name="cr.node.sql.distsql.select.count" title="DistSQL Reads" nonNegativeRate />
              <Metric name="cr.node.sql.update.count" title="Updates" nonNegativeRate />
              <Metric name="cr.node.sql.insert.count" title="Inserts" nonNegativeRate />
              <Metric name="cr.node.sql.delete.count" title="Deletes" nonNegativeRate />
            </Axis>
          </LineGraph>
          <LineGraph title="Exec Latency: 99th percentile"
          tooltip={`The 99th percentile of latency between query requests and responses
                    over a 1 minute period.
                    Values are displayed individually for each node on each node.`}>
            <Axis units={ AxisUnits.Duration }>
              {
                _.map(nodeIds, (node) =>
                  <Metric key={node}
                          name="cr.node.exec.latency-p99"
                          title={this.nodeAddress(node)}
                          sources={[node]}
                          downsampleMax />,
                )
              }
            </Axis>
          </LineGraph>
          <LineGraph title="Exec Latency: 90th percentile"
                    tooltip={`The 90th percentile of latency between query requests and responses
                              over a 1 minute period.
                              Values are displayed individually for each node on each node.`}>
            <Axis units={ AxisUnits.Duration }>
              {
                _.map(nodeIds, (node) =>
                  <Metric key={node}
                          name="cr.node.exec.latency-p90"
                          title={this.nodeAddress(node)}
                          sources={[node]}
                          downsampleMax />,
                )
              }
            </Axis>
          </LineGraph>
          <LineGraph title="Replicas per Store"
                    tooltip={`The number of replicas on each store.`}>
            <Axis>
              {
                _.map(nodeIds, (nid) =>
                  <Metric key={nid}
                          name="cr.store.replicas"
                          title={this.nodeAddress(nid)}
                          sources={this.storeIDsForNode(nid)}/>,
                )
              }
            </Axis>
          </LineGraph>
          <LineGraph title="Capacity" sources={storeSources} tooltip={`Summary of total and available capacity ${specifier}.`}>
            <Axis units={AxisUnits.Bytes}>
              <Metric name="cr.store.capacity" title="Capacity" />
              {
                // TODO(mrtracy): We really want to display a used capacity
                // stat, but that is not directly recorded. We either need to
                // start directly recording it, or add the ability to create
                // derived series.
              }
              <Metric name="cr.store.capacity.available" title="Available" />
            </Axis>
          </LineGraph>
        </GraphGroup>

        <GraphGroup groupId="node.runtime" hide={dashboard !== "runtime"}>
          <LineGraph title="Node Count" tooltip="The number of nodes active on the cluster.">
            <Axis>
              <Metric name="cr.node.liveness.livenodes" title="Live Nodes" aggregateMax />
            </Axis>
          </LineGraph>

          <LineGraph title="Memory Usage" sources={nodeSources} tooltip={<div>{`Memory in use ${specifier}:`}<dl>
            <dt>RSS</dt><dd>Total memory in use by CockroachDB</dd>
            <dt>Go Allocated</dt><dd>Memory allocated by the Go layer</dd>
            <dt>Go Total</dt><dd>Total memory managed by the Go layer</dd>
            <dt>C Allocated</dt><dd>Memory allocated by the C layer</dd>
            <dt>C Total</dt><dd>Total memory managed by the C layer</dd>
            </dl></div>}>
            <Axis units={ AxisUnits.Bytes }>
              <Metric name="cr.node.sys.rss" title="Total memory (RSS)" />
              <Metric name="cr.node.sys.go.allocbytes" title="Go Allocated" />
              <Metric name="cr.node.sys.go.totalbytes" title="Go Total" />
              <Metric name="cr.node.sys.cgo.allocbytes" title="CGo Allocated" />
              <Metric name="cr.node.sys.cgo.totalbytes" title="CGo Total" />
            </Axis>
          </LineGraph>

          <LineGraph title="Goroutine Count" sources={nodeSources} tooltip={`The number of Goroutines ${specifier}. This count should rise and fall based on load.`}>
            <Axis>
              <Metric name="cr.node.sys.goroutines" title="Goroutine Count" />
            </Axis>
          </LineGraph>

          {
            // TODO(mrtracy): The following two graphs are a good first example of a graph with
            // two axes; the two series should be highly correlated, but have different units.
          }
          <LineGraph title="GC Runs" sources={nodeSources} tooltip={`The number of times that Go’s garbage collector was invoked per second ${specifier}.`}>
            <Axis>
              <Metric name="cr.node.sys.gc.count" title="GC Runs" nonNegativeRate />
            </Axis>
          </LineGraph>

          <LineGraph title="GC Pause Time" sources={nodeSources} tooltip={`The amount of processor time used by Go’s garbage collector per second ${specifier}. During garbage collection, application code execution is paused.`}>
            <Axis units={ AxisUnits.Duration }>
              <Metric name="cr.node.sys.gc.pause.ns" title="GC Pause Time" nonNegativeRate />
            </Axis>
          </LineGraph>

          <LineGraph title="CPU Time" sources={nodeSources} tooltip={`The amount of CPU time used by CockroachDB (User) and system-level operations (Sys) ${specifier}.`}>
            <Axis units={ AxisUnits.Duration }>
              <Metric name="cr.node.sys.cpu.user.ns" title="User CPU Time" nonNegativeRate />
              <Metric name="cr.node.sys.cpu.sys.ns" title="Sys CPU Time" nonNegativeRate />
            </Axis>
          </LineGraph>

        </GraphGroup>

        <GraphGroup groupId="node.sql" hide={dashboard !== "sql"}>
          <LineGraph title="SQL Connections" sources={nodeSources} tooltip={`The total number of active SQL connections ${specifier}.`}>
            <Axis>
              <Metric name="cr.node.sql.conns" title="Client Connections" />
            </Axis>
          </LineGraph>

          <LineGraph title="SQL Byte Traffic" sources={nodeSources} tooltip={`The average amount of SQL client network traffic in bytes per second ${specifier}.`}>
            <Axis units={ AxisUnits.Bytes }>
              <Metric name="cr.node.sql.bytesin" title="Bytes In" nonNegativeRate />
              <Metric name="cr.node.sql.bytesout" title="Bytes Out" nonNegativeRate />
            </Axis>
          </LineGraph>

          <LineGraph title="SQL Queries" sources={nodeSources} tooltip={`The average number of SELECT, INSERT, UPDATE, and DELETE statements per second ${specifier}.`}>
            <Axis>
              <Metric name="cr.node.sql.select.count" title="Total Reads" nonNegativeRate />
              <Metric name="cr.node.sql.distsql.select.count" title="DistSQL Reads" nonNegativeRate />
              <Metric name="cr.node.sql.update.count" title="Updates" nonNegativeRate />
              <Metric name="cr.node.sql.insert.count" title="Inserts" nonNegativeRate />
              <Metric name="cr.node.sql.delete.count" title="Deletes" nonNegativeRate />
            </Axis>
          </LineGraph>

          <LineGraph title="Backend Statement Execution Latency: SQL, 99th percentile"
                       tooltip={`The latency of backend statements executed over 10 second periods ${specifier}.`}>
            <Axis units={ AxisUnits.Duration }>
              {
                _.map(nodeIds, (node) =>
                  <Metric key={node}
                          name="cr.node.sql.exec.latency-p99"
                          title={this.nodeAddress(node)}
                          sources={[node]}
                          downsampleMax />,
                )
              }
            </Axis>
          </LineGraph>

          <LineGraph title="Backend Statement Execution Latency: SQL, 90th percentile"
                       tooltip={`The latency of backend statements executed over 10 second periods ${specifier}.`}>
            <Axis units={ AxisUnits.Duration }>
              {
                _.map(nodeIds, (node) =>
                  <Metric key={node}
                          name="cr.node.sql.exec.latency-p90"
                          title={this.nodeAddress(node)}
                          sources={[node]}
                          downsampleMax />,
                )
              }
            </Axis>
          </LineGraph>

          <LineGraph title="Backend Statement Execution Latency: DistSQL, 99th percentile"
                       tooltip={`The latency of backend statements executed over 10 second periods ${specifier}.`}>
            <Axis units={ AxisUnits.Duration }>
              {
                _.map(nodeIds, (node) =>
                  <Metric key={node}
                          name="cr.node.sql.distsql.exec.latency-p99"
                          title={this.nodeAddress(node)}
                          sources={[node]}
                          downsampleMax />,
                )
              }
            </Axis>
          </LineGraph>

          <LineGraph title="Backend Statement Execution Latency: DistSQL, 90th percentile"
                       tooltip={`The latency of backend statements executed over 10 second periods ${specifier}.`}>
            <Axis units={ AxisUnits.Duration }>
              {
                _.map(nodeIds, (node) =>
                  <Metric key={node}
                          name="cr.node.sql.distsql.exec.latency-p90"
                          title={this.nodeAddress(node)}
                          sources={[node]}
                          downsampleMax />,
                )
              }
            </Axis>
          </LineGraph>

          <LineGraph title="Exec Latency: 99th percentile"
                    tooltip={`The 99th percentile of latency between query requests and responses
                              over a 1 minute period.
                              Values are displayed individually for each node on each node.`}>
            <Axis units={ AxisUnits.Duration }>
              {
                _.map(nodeIds, (node) =>
                  <Metric key={node}
                          name="cr.node.exec.latency-p99"
                          title={this.nodeAddress(node)}
                          sources={[node]}
                          downsampleMax />,
                )
              }
            </Axis>
          </LineGraph>

          <LineGraph title="Exec Latency: 90th percentile"
                    tooltip={`The 90th percentile of latency between query requests and responses
                              over a 1 minute period.
                              Values are displayed individually for each node on each node.`}>
            <Axis units={ AxisUnits.Duration }>
              {
                _.map(nodeIds, (node) =>
                  <Metric key={node}
                          name="cr.node.exec.latency-p90"
                          title={this.nodeAddress(node)}
                          sources={[node]}
                          downsampleMax />,
                )
              }
            </Axis>
          </LineGraph>

          <LineGraph title="Transactions" sources={nodeSources} tooltip={`The average number of transactions opened, committed, rolled back, or aborted per second ${specifier}.`}>
            <Axis>
              <Metric name="cr.node.sql.txn.begin.count" title="Begin" nonNegativeRate />
              <Metric name="cr.node.sql.txn.commit.count" title="Commits" nonNegativeRate />
              <Metric name="cr.node.sql.txn.rollback.count" title="Rollbacks" nonNegativeRate />
              <Metric name="cr.node.sql.txn.abort.count" title="Aborts" nonNegativeRate />
            </Axis>
          </LineGraph>

          <LineGraph title="Schema Changes" sources={nodeSources} tooltip={`The average number of DDL statements per second ${specifier}.`}>
            <Axis>
              <Metric name="cr.node.sql.ddl.count" title="DDL Statements" nonNegativeRate />
            </Axis>
          </LineGraph>
        </GraphGroup>

        <GraphGroup groupId="node.storage" hide={dashboard !== "storage"}>
          <LineGraph title="Capacity" sources={storeSources} tooltip={`Summary of total and available capacity ${specifier}.`}>
            <Axis>
              <Metric name="cr.store.capacity" title="Capacity" />
              {
                // TODO(mrtracy): We really want to display a used capacity
                // stat, but that is not directly recorded. We either need to
                // start directly recording it, or add the ability to create
                // derived series.
              }
              <Metric name="cr.store.capacity.available" title="Available" />
            </Axis>
          </LineGraph>

          <LineGraph title="Live Bytes" sources={storeSources} tooltip={`The amount of Live data used by both applications and the CockroachDB system ${specifier}.
                                                                    This excludes historical and deleted data.`}>
            <Axis units={ AxisUnits.Bytes }>
              <Metric name="cr.store.livebytes" title="Live" />
              <Metric name="cr.store.sysbytes" title="System" />
            </Axis>
          </LineGraph>

          <LineGraph title="RocksDB Read Amplification" sources={storeSources} tooltip={`RocksDB read amplification statistic; measures the average number of real read operations
                                                                                    executed per logical read operation ${specifier}.`}>
            <Axis>
              <Metric name="cr.store.rocksdb.read-amplification" title="Read Amplification" aggregateAvg />
            </Axis>
          </LineGraph>

          <LineGraph title="RocksDB SSTables" sources={storeSources} tooltip={`The number of RocksDB SSTables in use ${specifier}.`}>
            <Axis>
              <Metric name="cr.store.rocksdb.num-sstables" title="SSTables" />
            </Axis>
          </LineGraph>

          <LineGraph title="File Descriptors" sources={nodeSources} tooltip={`The number of open file descriptors ${specifier}, compared with the file descriptor limit.`}>
            <Axis>
              <Metric name="cr.node.sys.fd.open" title="Open" />
              <Metric name="cr.node.sys.fd.softlimit" title="Limit" />
            </Axis>
          </LineGraph>
        </GraphGroup>

        <GraphGroup groupId="node.replication" hide={dashboard !== "replication"}>
          <LineGraph title="Ranges" sources={storeSources}>
            <Axis>
              <Metric name="cr.store.ranges" title="Ranges" />
              <Metric name="cr.store.replicas.leaders" title="Leaders" />
              <Metric name="cr.store.replicas.leaseholders" title="Lease Holders" />
              <Metric name="cr.store.replicas.leaders_not_leaseholders" title="Leaders w/o Lease" />
              <Metric name="cr.store.ranges.unavailable" title="Unavailable" />
              <Metric name="cr.store.ranges.underreplicated" title="Under-replicated" />
            </Axis>
          </LineGraph>

          <LineGraph title="Replicas per Store"
                    tooltip={`The number of replicas on each store.`}>
            <Axis>
              {
                _.map(nodeIds, (nid) =>
                  <Metric key={nid}
                          name="cr.store.replicas"
                          title={this.nodeAddress(nid)}
                          sources={this.storeIDsForNode(nid)}/>,
                )
              }
            </Axis>
          </LineGraph>

          <LineGraph title="Leaseholders per Store"
                    tooltip={`The number of replicas on each store.`}>
            <Axis>
              {
                _.map(nodeIds, (nid) =>
                  <Metric key={nid}
                          name="cr.store.replicas.leaseholders"
                          title={this.nodeAddress(nid)}
                          sources={this.storeIDsForNode(nid)}/>,
                )
              }
            </Axis>
          </LineGraph>

          <LineGraph title="Replicas" sources={storeSources}>
            <Axis>
              <Metric name="cr.store.replicas" title="Replicas" />
              <Metric name="cr.store.replicas.quiescent" title="Quiescent" />
            </Axis>
          </LineGraph>

          <LineGraph title="Range Operations" sources={storeSources}>
            <Axis>
              <Metric name="cr.store.range.splits" title="Splits" nonNegativeRate />
              <Metric name="cr.store.range.adds" title="Adds" nonNegativeRate />
              <Metric name="cr.store.range.removes" title="Removes" nonNegativeRate />
            </Axis>
          </LineGraph>

          <LineGraph title="Snapshots" sources={storeSources}>
            <Axis>
              <Metric name="cr.store.range.snapshots.generated" title="Generated" nonNegativeRate />
              <Metric name="cr.store.range.snapshots.normal-applied" title="Normal-applied" nonNegativeRate />
              <Metric name="cr.store.range.snapshots.preemptive-applied" title="Preemptive-applied" nonNegativeRate />
              <Metric name="cr.store.replicas.reserved" title="Reserved" nonNegativeRate />
            </Axis>
          </LineGraph>
        </GraphGroup>

        <GraphGroup groupId="node.queues" hide={dashboard !== "queues"}>
          <LineGraph title="Queue Processing Failures" sources={storeSources}>
            <Axis>
              <Metric name="cr.store.queue.gc.process.failure" title="GC" nonNegativeRate />
              <Metric name="cr.store.queue.replicagc.process.failure" title="Replica GC" nonNegativeRate />
              <Metric name="cr.store.queue.replicate.process.failure" title="Replication" nonNegativeRate />
              <Metric name="cr.store.queue.split.process.failure" title="Split" nonNegativeRate />
              <Metric name="cr.store.queue.consistency.process.failure" title="Consistency" nonNegativeRate />
              <Metric name="cr.store.queue.raftlog.process.failure" title="Raft Log" nonNegativeRate />
              <Metric name="cr.store.queue.tsmaintenance.process.failure" title="Time Series Maintenance" nonNegativeRate />
            </Axis>
          </LineGraph>

          <LineGraph title="Queue Processing Times" sources={storeSources}>
            <Axis units={AxisUnits.Duration}>
              <Metric name="cr.store.queue.gc.processingnanos" title="GC" nonNegativeRate />
              <Metric name="cr.store.queue.replicagc.processingnanos" title="Replica GC" nonNegativeRate />
              <Metric name="cr.store.queue.replicate.processingnanos" title="Replication" nonNegativeRate />
              <Metric name="cr.store.queue.split.processingnanos" title="Split" nonNegativeRate />
              <Metric name="cr.store.queue.consistency.processingnanos" title="Consistency" nonNegativeRate />
              <Metric name="cr.store.queue.raftlog.processingnanos" title="Raft Log" nonNegativeRate />
              <Metric name="cr.store.queue.tsmaintenance.processingnanos" title="Time Series Maintenance" nonNegativeRate />
            </Axis>
          </LineGraph>

          {
            // TODO(mrtracy): The queues below should also have "processing
            // nanos" on the graph, but that has a time unit instead of a count
            // unit, and thus we need support for multi-axis graphs.
          }

          <LineGraph title="Replica GC Queue" sources={storeSources}>
            <Axis>
              <Metric name="cr.store.queue.replicagc.process.success" title="Successful Actions / sec" nonNegativeRate />
              <Metric name="cr.store.queue.replicagc.pending" title="Pending Actions" downsampleMax />
              <Metric name="cr.store.queue.replicagc.removereplica" title="Replicas Removed / sec" nonNegativeRate />
            </Axis>
          </LineGraph>

          <LineGraph title="Replication Queue" sources={storeSources}>
            <Axis>
              <Metric name="cr.store.queue.replicate.process.success" title="Successful Actions / sec" nonNegativeRate />
              <Metric name="cr.store.queue.replicate.pending" title="Pending Actions" />
              <Metric name="cr.store.queue.replicate.addreplica" title="Replicas Added / sec" nonNegativeRate />
              <Metric name="cr.store.queue.replicate.removereplica" title="Replicas Removed / sec" nonNegativeRate />
              <Metric name="cr.store.queue.replicate.removedeadreplica" title="Dead Replicas Removed / sec" nonNegativeRate />
              <Metric name="cr.store.queue.replicate.rebalancereplica" title="Replicas Rebalanced / sec" nonNegativeRate />
              <Metric name="cr.store.queue.replicate.transferlease" title="Leases Transferred / sec" nonNegativeRate />
              <Metric name="cr.store.queue.replicate.purgatory" title="Replicas in Purgatory" downsampleMax />
            </Axis>
          </LineGraph>

          <LineGraph title="Split Queue" sources={storeSources}>
            <Axis>
              <Metric name="cr.store.queue.split.process.success" title="Successful Actions / sec" nonNegativeRate />
              <Metric name="cr.store.queue.split.pending" title="Pending Actions" downsampleMax />
            </Axis>
          </LineGraph>

          <LineGraph title="GC Queue" sources={storeSources}>
            <Axis>
              <Metric name="cr.store.queue.gc.process.success" title="Successful Actions / sec" nonNegativeRate />
              <Metric name="cr.store.queue.gc.pending" title="Pending Actions" downsampleMax />
            </Axis>
          </LineGraph>

          <LineGraph title="Raft Log Queue" sources={storeSources}>
            <Axis>
              <Metric name="cr.store.queue.raftlog.process.success" title="Successful Actions / sec" nonNegativeRate />
              <Metric name="cr.store.queue.raftlog.pending" title="Pending Actions" downsampleMax />
            </Axis>
          </LineGraph>

          <LineGraph title="Consistency Checker Queue" sources={storeSources}>
            <Axis>
              <Metric name="cr.store.queue.consistency.process.success" title="Successful Actions / sec" nonNegativeRate />
              <Metric name="cr.store.queue.consistency.pending" title="Pending Actions" downsampleMax />
            </Axis>
          </LineGraph>

          <LineGraph title="Time Series Maintenance Queue" sources={storeSources}>
            <Axis>
              <Metric name="cr.store.queue.tsmaintenance.process.success" title="Successful Actions / sec" nonNegativeRate />
              <Metric name="cr.store.queue.tsmaintenance.pending" title="Pending Actions" downsampleMax />
            </Axis>
          </LineGraph>
        </GraphGroup>

        <GraphGroup groupId="node.requests" hide={dashboard !== "requests"}>
          <LineGraph title="Slow Distsender Requests" sources={nodeSources}>
            <Axis>
              <Metric name="cr.node.requests.slow.distsender" title="Slow Distsender Requests" nonNegativeRate />
            </Axis>
          </LineGraph>

          <LineGraph title="Slow Raft Proposals" sources={storeSources}>
            <Axis>
              <Metric name="cr.store.requests.slow.raft" title="Slow Raft Proposals" nonNegativeRate />
            </Axis>
          </LineGraph>

          <LineGraph title="Slow Lease Acquisitions" sources={storeSources}>
            <Axis>
              <Metric name="cr.store.requests.slow.lease" title="Slow Lease Acquisitions" nonNegativeRate />
            </Axis>
          </LineGraph>

          <LineGraph title="Slow Command Queue Entries" sources={storeSources}>
            <Axis>
              <Metric name="cr.store.requests.slow.commandqueue" title="Slow Command Queue Entries" nonNegativeRate />
            </Axis>
          </LineGraph>
        </GraphGroup>
      </div>
      <div className="l-columns__right">
        <SummaryBar>
          <SummaryLabel>Summary</SummaryLabel>
          <SummaryStat title={<span>Total Nodes <Link to="/cluster/nodes">View nodes list</Link></span>}
                       value={this.props.nodeCount} />
          <SummaryStat title="Capacity Used" value={capacityPercent}
                       format={(v) => `${d3.format(".2f")(v)}%`}
                       tooltip={`You are using ${Bytes(capacityUsed)} of ${Bytes(capacityTotal)}
                       storage capacity across all nodes.`} />
          <SummaryStat title="Unavailable ranges" value={this.props.unavailableRanges} />
          <SummaryMetricStat id="qps" title="Queries per second" format={d3.format(".1f")} >
            <Metric sources={nodeSources} name="cr.node.sql.query.count" title="Queries/Sec" nonNegativeRate />
          </SummaryMetricStat>
          <SummaryMetricStat id="p50" title="P50 latency" format={(n) => d3.format(".1f")(NanoToMilli(n)) + " ms"} >
            <Metric sources={nodeSources} name="cr.node.exec.latency-p50" aggregateMax downsampleMax />
          </SummaryMetricStat>
          <SummaryMetricStat id="p99" title="P99 latency" format={(n) => d3.format(".1f")(NanoToMilli(n)) + " ms"} >
            <Metric sources={nodeSources} name="cr.node.exec.latency-p99" aggregateMax downsampleMax />
          </SummaryMetricStat>
        </SummaryBar>
        <SummaryBar>
          <SummaryLabel>Events</SummaryLabel>
          <EventBox />
        </SummaryBar>
      </div>
    </div>;
  }
}

let nodeStatuses = (state: AdminUIState) => state.cachedData.nodes.data;

export let nodeSums = createSelector(
  nodeStatuses,
  (ns) => {
    let result = {
      nodeCount: 0,
      capacityAvailable: 0,
      capacityTotal: 0,
      usedBytes: 0,
      usedMem: 0,
      unavailableRanges: 0,
      replicas: 0,
    };
    if (_.isArray(ns)) {
      ns.forEach((n) => {
        result.nodeCount += 1;
        result.capacityAvailable += n.metrics.get(MetricConstants.availableCapacity);
        result.capacityTotal += n.metrics.get(MetricConstants.capacity);
        result.usedBytes += BytesUsed(n);
        result.usedMem += n.metrics.get(MetricConstants.rss);
        result.unavailableRanges += n.metrics.get(MetricConstants.unavailableRanges);
        result.replicas += n.metrics.get(MetricConstants.replicas);
      });
    }
    return result;
  },
);

let nodeIds = createSelector(
  nodeStatuses,
  (nss) => {
    return _.map(nss, (ns) => {
      return ns.desc.node_id.toString();
    });
  },
);

let nodeStatusByID = createSelector(
  nodeStatuses,
  (nss) => {
    let statuses: {[s: string]: NodeStatus} = {};
    _.each(nss, (ns) => {
      statuses[ns.desc.node_id.toString()] = ns;
    });
    return statuses;
  },
);

export default connect(
  (state: AdminUIState) => {
    let sums = nodeSums(state);
    return {
      nodeIds: nodeIds(state),
      nodeStatusByID: nodeStatusByID(state),
      nodeCount: sums.nodeCount,
      capacityAvailable: sums.capacityAvailable,
      capacityTotal: sums.capacityTotal,
      unavailableRanges: sums.unavailableRanges,
      nodesQueryValid: state.cachedData.nodes.valid,
    };
  },
  {
    refreshNodes,
  },
)(NodeGraphs);
