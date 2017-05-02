import _ from "lodash";
import * as React from "react";
import * as d3 from "d3";
import { RouterState, Link } from "react-router";
import { connect } from "react-redux";

import {
  nodeIDAttr, dashboardNameAttr,
} from "../util/constants";

import { AdminUIState } from "../redux/state";
import { refreshNodes, refreshLiveness } from "../redux/apiReducers";
import { nodesSummarySelector, NodesSummary } from "../redux/nodes";
import GraphGroup from "../components/graphGroup";
import {
  SummaryBar, SummaryLabel, SummaryStat, SummaryStatMessage, SummaryStatBreakdown, SummaryMetricStat,
} from "../components/summaryBar";
import Alerts from "./alerts";
import { Axis, AxisUnits } from "../components/graphs";
import { LineGraph } from "../components/linegraph";
import { Metric } from "../components/metric";
import { EventBox } from "../containers/events";
import { Bytes } from "../util/format";
import { NanoToMilli } from "../util/convert";

// The properties required by the NodeTotalsSummary component.
interface NodeTotalsSummaryProps {
  nodesSummary: NodesSummary;
}

/**
 * NodeTotalsSummary displays a high-level breakdown of the nodes on the cluster
 * and their current liveness status.
 */
class NodeTotalsSummary extends React.Component<NodeTotalsSummaryProps, {}> {
  render() {
    if (!this.props.nodesSummary || !this.props.nodesSummary.nodeSums) {
      return;
    }
    const { nodeCounts } = this.props.nodesSummary.nodeSums;
    let children: React.ReactNode;
    if (nodeCounts.dead > 0 || nodeCounts.suspect > 0) {
      children = <div>
        <SummaryStatBreakdown title="Healthy" value={nodeCounts.healthy} modifier="healthy" />
        <SummaryStatBreakdown title="Suspect" value={nodeCounts.suspect} modifier="suspect" />
        <SummaryStatBreakdown title="Dead" value={nodeCounts.dead} modifier="dead" />
      </div>;
    }
    return <SummaryStat title={<span>Total Nodes <Link to="/cluster/nodes">View nodes list</Link></span>}
                        value={nodeCounts.total}>
                        { children }
    </SummaryStat>;
  }
}

// The properties required by a NodeGraphs component.
interface NodeGraphsOwnProps {
  refreshNodes: typeof refreshNodes;
  refreshLiveness: typeof refreshLiveness;
  nodesQueryValid: boolean;
  livenessQueryValid: boolean;
  nodesSummary: NodesSummary;
}

type NodeGraphsProps = NodeGraphsOwnProps & RouterState;

/**
 * NodeGraphs renders the main content of the cluster graphs page.
 */
class NodeGraphs extends React.Component<NodeGraphsProps, {}> {
  static displayTimeScale = true;

  refresh(props = this.props) {
    if (!props.nodesQueryValid) {
      props.refreshNodes();
    }
    if (!props.livenessQueryValid) {
      props.refreshLiveness();
    }
  }

  componentWillMount() {
    this.refresh();
  }

  componentWillReceiveProps(props: NodeGraphsProps) {
    this.refresh(props);
  }

  nodeAddress(nid: string) {
    let ns = this.props.nodesSummary.nodeStatusByID[nid];
    if (!ns) {
      // This should only happen immediately after loading a page, and
      // associated graphs should display no data.
      return "unknown address";
    }
    return this.props.nodesSummary.nodeStatusByID[nid].desc.address.address_field;
  }

  storeIDsForNode(nid: string): string[] {
    let ns = this.props.nodesSummary.nodeStatusByID[nid];
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
    // node in the cluster using the nodeIDs collection. However, if a specific
    // node is already selected, these per-node graphs should only display data
    // only for the selected node.
    //
    // Similarly, if a single node is selected, we also need to restrict the
    // set of stores queried (only stores that belong to that node will be
    // queried).
    let { nodeIDs } = this.props.nodesSummary;
    if (nodeSources && nodeSources.length !== 0) {
      nodeIDs = nodeSources;
      storeSources = [];
      _.each(nodeSources, (nid) => {
        _.each(this.storeIDsForNode(nid), (sid) => storeSources.push(sid));
      });
    }

    let dashboard = this.props.params[dashboardNameAttr];
    let specifier = (nodeSources && nodeSources.length === 1) ? `on node ${nodeSources[0]}` : "across all nodes";

    // Capacity math used in the summary status section.
    let { capacityTotal, capacityAvailable } = this.props.nodesSummary.nodeSums;
    let capacityUsed = capacityTotal - capacityAvailable;
    let capacityPercent = capacityTotal !== 0 ? (capacityUsed / capacityTotal * 100) : 100;

    return <div className="section l-columns">
      <div className="chart-group l-columns__left">
        <GraphGroup groupId="node.overview" hide={dashboard !== "overview"}>
          <LineGraph title="SQL Queries" sources={nodeSources} tooltip={`The average number of SELECT, INSERT, UPDATE, and DELETE statements per second ${specifier}.`}>
            <Axis>
              <Metric name="cr.node.sql.select.count" title="Total Reads" nonNegativeRate />
              <Metric name="cr.node.sql.distsql.select.count" title="DistSQL Reads" nonNegativeRate />
              <Metric name="cr.node.sql.update.count" title="Updates" nonNegativeRate />
              <Metric name="cr.node.sql.insert.count" title="Inserts" nonNegativeRate />
              <Metric name="cr.node.sql.delete.count" title="Deletes" nonNegativeRate />
            </Axis>
          </LineGraph>

          <LineGraph title="Service Latency: SQL, 99th percentile"
                       tooltip={(
                    <div>
                      Over the last minute, this node executed 99% of queries within this time.&nbsp;
                      <em>
                      This time does not include network latency between the node and client.
                      </em>
                    </div>)}
          >
            <Axis units={ AxisUnits.Duration }>
              {
                _.map(nodeIDs, (node) =>
                  <Metric key={node}
                          name="cr.node.sql.service.latency-p99"
                          title={this.nodeAddress(node)}
                          sources={[node]}
                          downsampleMax />,
                )
              }
            </Axis>
          </LineGraph>

          <LineGraph title="Replicas per Node"
                    tooltip={(
                    <div>
                      The number of range replicas stored on this node.&nbsp;
                      <em>
                      Ranges are subsets of your data, which are replicated to ensure survivability.
                      </em>
                    </div>)}
          >
            <Axis>
              {
                _.map(nodeIDs, (nid) =>
                  <Metric key={nid}
                          name="cr.store.replicas"
                          title={this.nodeAddress(nid)}
                          sources={this.storeIDsForNode(nid)}/>,
                )
              }
            </Axis>
          </LineGraph>
          <LineGraph title="Capacity" sources={storeSources} tooltip={(
              <div>
                <dl>
                  <dt>Capacity</dt>
                  <dd>Total disk space available {specifier} to CockroachDB. <em>Control this value per node with the <code><a href="https://www.cockroachlabs.com/docs/start-a-node.html#flags" target="_blank">--store</a></code> flag.</em></dd>
                  <dt>Available</dt>
                  <dd>Free disk space available {specifier} to CockroachDB.</dd>
                </dl>
              </div>
            )}
          >
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

          <LineGraph
            title="Memory Usage"
            sources={nodeSources}
            tooltip={(
              <div>
                {`Memory in use ${specifier}:`}
                <dl>
                  <dt>RSS</dt>
                  <dd>Total memory in use by CockroachDB</dd>
                  <dt>Go Allocated</dt>
                  <dd>Memory allocated by the Go layer</dd>
                  <dt>Go Total</dt>
                  <dd>Total memory managed by the Go layer</dd>
                  <dt>C Allocated</dt>
                  <dd>Memory allocated by the C layer</dd>
                  <dt>C Total</dt>
                  <dd>Total memory managed by the C layer</dd>
                </dl>
              </div>
            )}
          >
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
              <Metric name="cr.node.sys.gc.pause.ns" title="GC Pause Time" nonNegativeRate />
            </Axis>
          </LineGraph>

        </GraphGroup>

        <GraphGroup groupId="node.sql" hide={dashboard !== "sql"}>
          <LineGraph title="SQL Connections" sources={nodeSources} tooltip={`The total number of active SQL connections ${specifier}.`}>
            <Axis>
              <Metric name="cr.node.sql.conns" title="Client Connections" />
            </Axis>
          </LineGraph>

          <LineGraph title="SQL Byte Traffic" sources={nodeSources} tooltip={`The total amount of SQL client network traffic in bytes per second ${specifier}.`}>
            <Axis units={ AxisUnits.Bytes }>
              <Metric name="cr.node.sql.bytesin" title="Bytes In" nonNegativeRate />
              <Metric name="cr.node.sql.bytesout" title="Bytes Out" nonNegativeRate />
            </Axis>
          </LineGraph>

          <LineGraph title="SQL Queries" sources={nodeSources} tooltip={`The total number of SELECT, INSERT, UPDATE, and DELETE statements per second ${specifier}.`}>
            <Axis>
              <Metric name="cr.node.sql.select.count" title="Total Reads" nonNegativeRate />
              <Metric name="cr.node.sql.distsql.select.count" title="DistSQL Reads" nonNegativeRate />
              <Metric name="cr.node.sql.update.count" title="Updates" nonNegativeRate />
              <Metric name="cr.node.sql.insert.count" title="Inserts" nonNegativeRate />
              <Metric name="cr.node.sql.delete.count" title="Deletes" nonNegativeRate />
            </Axis>
          </LineGraph>

          <LineGraph title="Service Latency: SQL, 99th percentile"
                       tooltip={(
                    <div>
                      Over the last minute, this node executed 99% of queries within this time.&nbsp;
                      <em>
                      This time does not include network latency between the node and client.
                      </em>
                    </div>)}
            >
            <Axis units={ AxisUnits.Duration }>
              {
                _.map(nodeIDs, (node) =>
                  <Metric key={node}
                          name="cr.node.sql.service.latency-p99"
                          title={this.nodeAddress(node)}
                          sources={[node]}
                          downsampleMax />,
                )
              }
            </Axis>
          </LineGraph>

          <LineGraph title="Service Latency: SQL, 90th percentile"
                       tooltip={(
                    <div>
                      Over the last minute, this node executed 90% of queries within this time.&nbsp;
                      <em>
                      This time does not include network latency between the node and client.
                      </em>
                    </div>)}
          >
            <Axis units={ AxisUnits.Duration }>
              {
                _.map(nodeIDs, (node) =>
                  <Metric key={node}
                          name="cr.node.sql.service.latency-p90"
                          title={this.nodeAddress(node)}
                          sources={[node]}
                          downsampleMax />,
                )
              }
            </Axis>
          </LineGraph>

          <LineGraph title="Service Latency: DistSQL, 99th percentile"
                       tooltip={`The latency of distributed SQL statements serviced over 10 second periods ${specifier}.`}>
            <Axis units={ AxisUnits.Duration }>
              {
                _.map(nodeIDs, (node) =>
                  <Metric key={node}
                          name="cr.node.sql.distsql.service.latency-p99"
                          title={this.nodeAddress(node)}
                          sources={[node]}
                          downsampleMax />,
                )
              }
            </Axis>
          </LineGraph>

          <LineGraph title="Service Latency: DistSQL, 90th percentile"
                       tooltip={`The latency of distributed SQL statements serviced over 10 second periods ${specifier}.`}>
            <Axis units={ AxisUnits.Duration }>
              {
                _.map(nodeIDs, (node) =>
                  <Metric key={node}
                          name="cr.node.sql.distsql.service.latency-p90"
                          title={this.nodeAddress(node)}
                          sources={[node]}
                          downsampleMax />,
                )
              }
            </Axis>
          </LineGraph>

          <LineGraph title="Execution Latency: 99th percentile"
                    tooltip={`The 99th percentile of latency between query requests and responses
                              over a 1 minute period.
                              Values are displayed individually for each node on each node.`}>
            <Axis units={ AxisUnits.Duration }>
              {
                _.map(nodeIDs, (node) =>
                  <Metric key={node}
                          name="cr.node.exec.latency-p99"
                          title={this.nodeAddress(node)}
                          sources={[node]}
                          downsampleMax />,
                )
              }
            </Axis>
          </LineGraph>

          <LineGraph title="Execution Latency: 90th percentile"
                    tooltip={`The 90th percentile of latency between query requests and responses
                              over a 1 minute period.
                              Values are displayed individually for each node on each node.`}>
            <Axis units={ AxisUnits.Duration }>
              {
                _.map(nodeIDs, (node) =>
                  <Metric key={node}
                          name="cr.node.exec.latency-p90"
                          title={this.nodeAddress(node)}
                          sources={[node]}
                          downsampleMax />,
                )
              }
            </Axis>
          </LineGraph>

          <LineGraph title="Transactions" sources={nodeSources} tooltip={`The total number of transactions opened, committed, rolled back, or aborted per second ${specifier}.`}>
            <Axis>
              <Metric name="cr.node.sql.txn.begin.count" title="Begin" nonNegativeRate />
              <Metric name="cr.node.sql.txn.commit.count" title="Commits" nonNegativeRate />
              <Metric name="cr.node.sql.txn.rollback.count" title="Rollbacks" nonNegativeRate />
              <Metric name="cr.node.sql.txn.abort.count" title="Aborts" nonNegativeRate />
            </Axis>
          </LineGraph>

          <LineGraph title="Schema Changes" sources={nodeSources} tooltip={`The total number of DDL statements per second ${specifier}.`}>
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

          <LineGraph title="Log Commit Latency: 99th Percentile" sources={storeSources} tooltip={`The 99th %ile latency for commits to the Raft Log.`}>
            <Axis units={AxisUnits.Duration}>
              {
                _.map(nodeIDs, (nid) =>
                  <Metric key={nid}
                          name="cr.store.raft.process.logcommit.latency-p99"
                          title={this.nodeAddress(nid)}
                          sources={this.storeIDsForNode(nid)} />,
                )
              }
            </Axis>
          </LineGraph>

          <LineGraph title="Command Commit Latency: 99th Percentile" sources={storeSources} tooltip={`The 99th %ile latency for commits of Raft commands.`}>
            <Axis units={AxisUnits.Duration}>
              {
                _.map(nodeIDs, (nid) =>
                  <Metric key={nid}
                          name="cr.store.raft.process.commandcommit.latency-p99"
                          title={this.nodeAddress(nid)}
                          sources={this.storeIDsForNode(nid)} />,
                )
              }
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
                _.map(nodeIDs, (nid) =>
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
                _.map(nodeIDs, (nid) =>
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

        <GraphGroup groupId="node.distributed" hide={dashboard !== "distributed"}>
          <LineGraph title="Batches" sources={nodeSources}>
            <Axis>
              <Metric name="cr.node.distsender.batches" title="Batches" nonNegativeRate />
              <Metric name="cr.node.distsender.batches.partial" title="Partial Batches" nonNegativeRate />
            </Axis>
          </LineGraph>

          <LineGraph title="RPCs" sources={nodeSources}>
            <Axis>
              <Metric name="cr.node.distsender.rpc.sent" title="RPCs Sent" nonNegativeRate />
              <Metric name="cr.node.distsender.rpc.sent.local" title="Local Fast-path" nonNegativeRate />
            </Axis>
          </LineGraph>

          <LineGraph title="RPC Errors" sources={nodeSources}>
            <Axis>
              <Metric name="cr.node.distsender.rpc.sent.sendnexttimeout" title="RPC Timeouts" nonNegativeRate />
              <Metric name="cr.node.distsender.rpc.sent.nextreplicaerror" title="Replica Errors" nonNegativeRate />
              <Metric name="cr.node.distsender.errors.notleaseholder" title="Not Leaseholder Errors" nonNegativeRate />
            </Axis>
          </LineGraph>

          <LineGraph title="KV Transactions" sources={nodeSources}>
            <Axis>
              <Metric name="cr.node.txn.commits" title="Committed" nonNegativeRate />
              <Metric name="cr.node.txn.commits1PC" title="Fast-path Committed" nonNegativeRate />
              <Metric name="cr.node.txn.aborts" title="Aborted" nonNegativeRate />
              <Metric name="cr.node.txn.abandons" title="Abandoned" nonNegativeRate />
            </Axis>
          </LineGraph>

          <LineGraph title="KV Transaction Restarts" sources={nodeSources}>
            <Axis>
              <Metric name="cr.node.txn.restarts.writetooold" title="Write Too Old" nonNegativeRate />
              <Metric name="cr.node.txn.restarts.deleterange" title="Forwarded Timestamp (delete range)" nonNegativeRate />
              <Metric name="cr.node.txn.restarts.serializable" title="Forwarded Timestamp (iso=serializable)" nonNegativeRate />
              <Metric name="cr.node.txn.restarts.possiblereplay" title="Possible Replay" nonNegativeRate />
            </Axis>
          </LineGraph>

          <LineGraph title="KV Transaction Durations: 99th percentile"
                    tooltip={`The 99th percentile of transaction durations over a 1 minute period.
                              Values are displayed individually for each node on each node.`}>
            <Axis units={ AxisUnits.Duration }>
              {
                _.map(nodeIDs, (node) =>
                  <Metric key={node}
                          name="cr.node.txn.durations-p99"
                          title={this.nodeAddress(node)}
                          sources={[node]}
                          downsampleMax />,
                )
              }
            </Axis>
          </LineGraph>

          <LineGraph title="KV Transaction Durations: 90th percentile"
                    tooltip={`The 90th percentile of transaction durations over a 1 minute period.
                              Values are displayed individually for each node on each node.`}>
            <Axis units={ AxisUnits.Duration }>
              {
                _.map(nodeIDs, (node) =>
                  <Metric key={node}
                          name="cr.node.txn.durations-p90"
                          title={this.nodeAddress(node)}
                          sources={[node]}
                          downsampleMax />,
                )
              }
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
        <Alerts />
        <SummaryBar>
          <SummaryLabel>Summary</SummaryLabel>
          <NodeTotalsSummary nodesSummary={this.props.nodesSummary}/>
          <SummaryStat title="Capacity Used" value={capacityPercent}
                       format={(v) => `${d3.format(".2f")(v)}%`}>
            <SummaryStatMessage message={`You are using ${Bytes(capacityUsed)} of ${Bytes(capacityTotal)}
                                          storage capacity across all nodes.`} />
          </SummaryStat>
          <SummaryStat title="Unavailable ranges" value={this.props.nodesSummary.nodeSums.unavailableRanges} />
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

export default connect(
  (state: AdminUIState) => {
    return {
      nodesSummary: nodesSummarySelector(state),
      nodesQueryValid: state.cachedData.nodes.valid,
      livenessQueryValid: state.cachedData.nodes.valid,
    };
  },
  {
    refreshNodes,
    refreshLiveness,
  },
)(NodeGraphs);
