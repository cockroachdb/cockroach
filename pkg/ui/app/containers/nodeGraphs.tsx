import * as React from "react";
import * as d3 from "d3";

import GraphGroup from "../components/graphGroup";
import { LineGraph, Axis, Metric } from "../components/linegraph";
import { StackedAreaGraph } from "../components/stackedgraph";
import { Bytes } from "../util/format";
import { NanoToMilli } from "../util/convert";

interface NodeGraphsOwnProps {
  groupId: string;
  nodeIds: string[];
}

/**
 * Renders the main content of the help us page.
 */
export default class NodeGraphs extends React.Component<NodeGraphsOwnProps, {}> {
  static displayTimeScale = true;
  render() {
    let sources: string[] = this.props.nodeIds || null;
    let specifier = (sources && sources.length === 1) ? `on node ${sources[0]}` : "across all nodes";

    return <div className="section node">
      <div className="charts">
        <GraphGroup groupId="node.activity" hide={this.props.groupId !== "node.activity"}>
          <h2>Activity</h2>

          <LineGraph title="SQL Connections" sources={sources} tooltip={`The total number of active SQL connections ${specifier}.`}>
              <Axis format={ d3.format(".1f") }>
                <Metric name="cr.node.sql.conns" title="Client Connections" />
              </Axis>
            </LineGraph>

            <LineGraph title="SQL Traffic" sources={sources} tooltip={`The average amount of SQL client network traffic in bytes per second ${specifier}.`}>
              <Axis format={ Bytes }>
                <Metric name="cr.node.sql.bytesin" title="Bytes In" nonNegativeRate />
                <Metric name="cr.node.sql.bytesout" title="Bytes Out" nonNegativeRate />
              </Axis>
            </LineGraph>

            <LineGraph title="Queries Per Second" sources={sources} tooltip={`The average number of SQL queries per second ${specifier}.`}>
              <Axis format={ d3.format(".1f") }>
                <Metric name="cr.node.sql.query.count" title="Queries/Sec" nonNegativeRate />
              </Axis>
            </LineGraph>

            <LineGraph title="Live Bytes" sources={sources} tooltip={`The amount of storage space used by live (non-historical) data ${specifier}.`}>
              <Axis format={ Bytes }>
                <Metric name="cr.store.livebytes" title="Live Bytes" />
              </Axis>
            </LineGraph>

            <LineGraph title="Query Time"
                       subtitle="(Max Per Percentile)"
                       tooltip={`The latency between query requests and responses over a 1 minute period.
                                 Percentiles are first calculated on each node.
                                 For each percentile, the maximum latency across all nodes is then shown.`}
                       sources={sources}>
              <Axis format={ (n: number) => d3.format(".1f")(NanoToMilli(n)) } label="Milliseconds">
                <Metric name="cr.node.exec.latency-max" title="Max Latency"
                        aggregateMax downsampleMax />
                <Metric name="cr.node.exec.latency-p99" title="99th percentile latency"
                        aggregateMax downsampleMax />
                <Metric name="cr.node.exec.latency-p90" title="90th percentile latency"
                        aggregateMax downsampleMax />
                <Metric name="cr.node.exec.latency-p50" title="50th percentile latency"
                        aggregateMax downsampleMax />
              </Axis>
            </LineGraph>

            <LineGraph title="GC Pause Time" sources={sources} tooltip={`The ${sources ? "average and maximum" : ""} amount of processor time used by Go’s garbage collector per second ${specifier}. During garbage collection, application code execution is paused.`}>
              <Axis label="Milliseconds" format={ (n) => d3.format(".1f")(NanoToMilli(n)) }>
                <Metric name="cr.node.sys.gc.pause.ns" title={`${sources ? "" : "Avg "}Time`} aggregateAvg nonNegativeRate />
                { (sources && sources[0]) ? null : <Metric name="cr.node.sys.gc.pause.ns" title="Max Time" aggregateMax nonNegativeRate /> }
              </Axis>
            </LineGraph>

          </GraphGroup>
        <GraphGroup groupId="node.queries" hide={this.props.groupId !== "node.queries"}>
          <h2>SQL Queries</h2>
            <LineGraph title="Reads" sources={sources} tooltip={`The average number of SELECT statements per second ${specifier}.`}>
              <Axis format={ d3.format(".1f") }>
                <Metric name="cr.node.sql.select.count" title="Selects" nonNegativeRate />
              </Axis>
            </LineGraph>

            <LineGraph title="Writes" sources={sources} tooltip={`The average number of INSERT, UPDATE, and DELETE statements per second across ${specifier}.`}>
              <Axis format={ d3.format(".1f") }>
                <Metric name="cr.node.sql.update.count" title="Updates" nonNegativeRate />
                <Metric name="cr.node.sql.insert.count" title="Inserts" nonNegativeRate />
                <Metric name="cr.node.sql.delete.count" title="Deletes" nonNegativeRate />
              </Axis>
            </LineGraph>

            <LineGraph title="Transactions" sources={sources} tooltip={`The average number of transactions committed, rolled back, or aborted per second ${specifier}.`}>
              <Axis format={ d3.format(".1f") }>
                <Metric name="cr.node.sql.txn.commit.count" title="Commits" nonNegativeRate />
                <Metric name="cr.node.sql.txn.rollback.count" title="Rollbacks" nonNegativeRate />
                <Metric name="cr.node.sql.txn.abort.count" title="Aborts" nonNegativeRate />
              </Axis>
            </LineGraph>

            <LineGraph title="Schema Changes" sources={sources} tooltip={`The average number of DDL statements per second ${specifier}.`}>
              <Axis format={ d3.format(".1f") }>
                <Metric name="cr.node.sql.ddl.count" title="DDL Statements" nonNegativeRate />
              </Axis>
            </LineGraph>

          </GraphGroup>

          <GraphGroup groupId="node.resources" hide={this.props.groupId !== "node.resources"}>
            <h2>System Resources</h2>
            <StackedAreaGraph title="CPU Usage" sources={sources} tooltip={`The average percentage of CPU used by CockroachDB (User %) and system-level operations (Sys %) ${specifier}.`}>
              <Axis format={ d3.format(".2%") }>
                <Metric name="cr.node.sys.cpu.user.percent" aggregateAvg title="CPU User %" />
                <Metric name="cr.node.sys.cpu.sys.percent" aggregateAvg title="CPU Sys %" />
              </Axis>
            </StackedAreaGraph>

            <LineGraph title="Memory Usage" sources={sources} tooltip={<div>{`Memory in use ${specifier}:`}<dl>
            <dt>RSS</dt><dd>Total memory in use by CockroachDB</dd>
            <dt>Go Allocated</dt><dd>Memory allocated by the Go layer</dd>
            <dt>Go Total</dt><dd>Total memory managed by the Go layer</dd>
            <dt>C Allocated</dt><dd>Memory allocated by the C layer</dd>
            <dt>C Total</dt><dd>Total memory managed by the C layer</dd>
            </dl></div>}>
              <Axis format={ Bytes }>
                <Metric name="cr.node.sys.rss" title="Total memory (RSS)" />
                <Metric name="cr.node.sys.go.allocbytes" title="Go Allocated" />
                <Metric name="cr.node.sys.go.totalbytes" title="Go Total" />
                <Metric name="cr.node.sys.cgo.allocbytes" title="C Allocated" />
                <Metric name="cr.node.sys.cgo.totalbytes" title="C Total" />
              </Axis>
            </LineGraph>

            <StackedAreaGraph title="SQL Memory" sources={sources}>
              <Axis format={ Bytes }>
                <Metric name="cr.node.sql.mon.client.cur" title="Clients" />
              </Axis>
            </StackedAreaGraph>

            <LineGraph title="Goroutine Count" sources={sources} tooltip={`The number of Goroutines ${specifier}. This count should rise and fall based on load.`}>
              <Axis format={ d3.format(".1f") }>
                <Metric name="cr.node.sys.goroutines" title="Goroutine Count" />
              </Axis>
            </LineGraph>

            <LineGraph title="Cgo Calls" sources={sources} tooltip={`The average number of calls from Go to C per second ${specifier}.`}>
              <Axis format={ d3.format(".1f") }>
                <Metric name="cr.node.sys.cgocalls" title="Cgo Calls" nonNegativeRate />
              </Axis>
            </LineGraph>

          </GraphGroup>

          <GraphGroup groupId="node.internals" hide={this.props.groupId !== "node.internals"}>
            <h2>Advanced Internals</h2>
            <StackedAreaGraph title="Key/Value Transactions" sources={sources}>
              <Axis label="transactions/sec" format={ d3.format(".1f") }>
                <Metric name="cr.node.txn.commits-count" title="Commits" nonNegativeRate />
                <Metric name="cr.node.txn.commits1PC-count" title="Fast 1PC" nonNegativeRate />
                <Metric name="cr.node.txn.aborts-count" title="Aborts" nonNegativeRate />
                <Metric name="cr.node.txn.abandons-count" title="Abandons" nonNegativeRate />
              </Axis>
            </StackedAreaGraph>

            <StackedAreaGraph title="Node Liveness" sources={sources}>
              <Axis label="Count" format={ d3.format(".1f") }>
                <Metric name="cr.node.liveness.heartbeatsuccesses" title="Heartbeat Successes" nonNegativeRate />
                <Metric name="cr.node.liveness.heartbeatfailures" title="Heartbeat Failures" nonNegativeRate />
                <Metric name="cr.node.liveness.epochincrements" title="Epoch Increments" nonNegativeRate />
              </Axis>
            </StackedAreaGraph>

            <LineGraph title="Engine Memory Usage" sources={sources}>
              <Axis format={ Bytes }>
                <Metric name="cr.store.rocksdb.block.cache.usage" title="Block Cache" />
                <Metric name="cr.store.rocksdb.block.cache.pinned-usage" title="Iterators" />
                <Metric name="cr.store.rocksdb.memtable.total-size" title="Memtable" />
                <Metric name="cr.store.rocksdb.table-readers-mem-estimate" title="Index" />
              </Axis>
            </LineGraph>

            <StackedAreaGraph title="Block Cache Hits/Misses" sources={sources}>
              <Axis format={ d3.format(".1f") }>
                <Metric name="cr.store.rocksdb.block.cache.hits"
                        title="Cache Hits"
                        nonNegativeRate />
                <Metric name="cr.store.rocksdb.block.cache.misses"
                        title="Cache Missses"
                        nonNegativeRate />
              </Axis>
            </StackedAreaGraph>

            <StackedAreaGraph title="Range Events" sources={sources}>
              <Axis format={ d3.format(".1f") }>
                <Metric name="cr.store.range.splits" title="Splits" nonNegativeRate />
                <Metric name="cr.store.range.adds" title="Adds" nonNegativeRate />
                <Metric name="cr.store.range.removes" title="Removes" nonNegativeRate />
              </Axis>
            </StackedAreaGraph>

            <LineGraph title="Flushes and Compactions" sources={sources}>
              <Axis format={ d3.format(".1f") }>
                <Metric name="cr.store.rocksdb.flushes" title="Flushes" nonNegativeRate />
                <Metric name="cr.store.rocksdb.compactions" title="Compactions" nonNegativeRate />
              </Axis>
            </LineGraph>

            <LineGraph title="Bloom Filter Prefix" sources={sources}>
              <Axis format={ d3.format(".1f") }>
                <Metric name="cr.store.rocksdb.bloom.filter.prefix.checked"
                        title="Checked"
                        nonNegativeRate />
                <Metric name="cr.store.rocksdb.bloom.filter.prefix.useful"
                        title="Useful"
                        nonNegativeRate />
              </Axis>
            </LineGraph>

            <LineGraph title="Read Amplification" sources={sources}>
              <Axis format={ d3.format(".1f") }>
                <Metric name="cr.store.rocksdb.read-amplification" title="Read Amplification" />
              </Axis>
            </LineGraph>

            <StackedAreaGraph title="Raft Time" sources={sources}>
              <Axis label="Milliseconds" format={ (n) => d3.format(".1f")(NanoToMilli(n)) }>
                <Metric name="cr.store.raft.process.workingnanos" title="Working" nonNegativeRate />
                <Metric name="cr.store.raft.process.tickingnanos" title="Ticking" nonNegativeRate />
              </Axis>
            </StackedAreaGraph>

            <StackedAreaGraph title="Raft Messages received" sources={sources}>
              <Axis label="Count" format={ d3.format(".1f") }>
                <Metric name="cr.store.raft.rcvd.prop" title="MsgProp" nonNegativeRate />
                <Metric name="cr.store.raft.rcvd.app" title="MsgApp" nonNegativeRate />
                <Metric name="cr.store.raft.rcvd.appresp" title="MsgAppResp" nonNegativeRate />
                <Metric name="cr.store.raft.rcvd.vote" title="MsgVote" nonNegativeRate />
                <Metric name="cr.store.raft.rcvd.voteresp" title="MsgVoteResp" nonNegativeRate />
                <Metric name="cr.store.raft.rcvd.snap" title="MsgSnap" nonNegativeRate />
                <Metric name="cr.store.raft.rcvd.heartbeat" title="MsgHeartbeat" nonNegativeRate />
                <Metric name="cr.store.raft.rcvd.heartbeatresp" title="MsgHeartbeatResp" nonNegativeRate />
                <Metric name="cr.store.raft.rcvd.transferleader" title="MsgTransferLeader" nonNegativeRate />
                <Metric name="cr.store.raft.rcvd.timeoutnow" title="MsgTimeoutNow" nonNegativeRate />
                <Metric name="cr.store.raft.rcvd.dropped" title="MsgDropped" nonNegativeRate />
              </Axis>
            </StackedAreaGraph>

            <LineGraph title="GCInfo metrics" sources={sources}>
              <Axis label="Count" format={ d3.format(".1f") }>
                <Metric name="cr.store.queue.gc.info.numkeysaffected" title="NumKeysAffected" nonNegativeRate />
                <Metric name="cr.store.queue.gc.info.intentsconsidered" title="IntentsConsidered" nonNegativeRate />
                <Metric name="cr.store.queue.gc.info.intenttxns" title="IntentTxns" nonNegativeRate />
                <Metric name="cr.store.queue.gc.info.transactionspanscanned" title="TransactionSpanScanned" nonNegativeRate />
                <Metric name="cr.store.queue.gc.info.transactionspangcaborted" title="TransactionSpanGCAborted" nonNegativeRate />
                <Metric name="cr.store.queue.gc.info.transactionspangccommitted" title="TransactionSpanGCCommitted" nonNegativeRate />
                <Metric name="cr.store.queue.gc.info.transactionspangcpending" title="TransactionSpanGCPending" nonNegativeRate />
                <Metric name="cr.store.queue.gc.info.abortspanscanned" title="AbortSpanScanned" nonNegativeRate />
                <Metric name="cr.store.queue.gc.info.abortspanconsidered" title="AbortSpanConsidered" nonNegativeRate />
                <Metric name="cr.store.queue.gc.info.abortspangcnum" title="AbortSpanGCNum" nonNegativeRate />
                <Metric name="cr.store.queue.gc.info.pushtxn" title="PushTxn" nonNegativeRate />
                <Metric name="cr.store.queue.gc.info.resolvetotal" title="ResolveTotal" nonNegativeRate />
                <Metric name="cr.store.queue.gc.info.resovlesuccess" title="ResolveSuccess" nonNegativeRate />
              </Axis>
            </LineGraph>

            <LineGraph title="Raft Transport Queue Pending Count" sources={sources}>
              <Axis format={ d3.format(".1f") }>
                <Metric name="cr.store.raft.enqueued.pending" title="Outstanding message count in the Raft Transport queue to be sent over the network" />
                <Metric name="cr.store.raft.heartbeats.pending" title="Outstanding individual heartbeats in the Raft Transport queue that have been coalesced" />
              </Axis>
            </LineGraph>

            <LineGraph title="Replicas: Details" sources={sources}>
              <Axis format={ d3.format(".1f") }>
                <Metric name="cr.store.replicas.leaders" title="Leaders" />
                <Metric name="cr.store.replicas.leaseholders" title="Lease Holders" />
                <Metric name="cr.store.replicas.leaders_not_leaseholders" title="Leaders w/o Lease" />
                <Metric name="cr.store.replicas.quiescent" title="Quiescent" />
              </Axis>
            </LineGraph>

            <LineGraph title="Raft Ticks" sources={sources}>
              <Axis format={ d3.format(".1f") }>
                <Metric name="cr.store.raft.ticks" title="Raft Ticks" nonNegativeRate />
              </Axis>
            </LineGraph>

            <LineGraph title="Critical Section Time"
                       tooltip={`The maximum duration (capped at 1s) for which the corresponding mutex was held in the last minute ${specifier}.`}
                       sources={sources}>
              <Axis format={ (n: number) => d3.format(".1f")(NanoToMilli(n)) } label="Milliseconds">
                <Metric name="cr.store.mutex.storenanos-max" title="StoreMu"
                        aggregateMax downsampleMax />
                <Metric name="cr.store.mutex.schedulernanos-max" title="SchedulerMu"
                        aggregateMax downsampleMax />
                <Metric name="cr.store.mutex.replicananos-max" title="ReplicaMu"
                        aggregateMax downsampleMax />
                <Metric name="cr.store.mutex.raftnanos-max" title="RaftMu"
                        aggregateMax downsampleMax />
              </Axis>
            </LineGraph>

            <StackedAreaGraph title="SQL Memory (detailed)" sources={sources}>
              <Axis format={ Bytes }>
                <Metric name="cr.node.sql.mon.client.cur" title="Clients" />
                <Metric name="cr.node.sql.mon.admin.cur" title="Admin" />
                <Metric name="cr.node.sql.mon.internal.cur" title="Internal" />
              </Axis>
            </StackedAreaGraph>

            <LineGraph title="SQL Session Cumulative Max Size"
                       subtitle="(log10(Max) Per Percentile)"
                       tooltip={`The maximum memory usage per SQL session (including session-bound and txn-bound data), displayed as log(max).
                                 Percentiles are first calculated on each node.
                                 For each percentile, the maximum usage across all nodes is then shown.`}
                       sources={sources}>
              <Axis format={ (n: number) => d3.format(".3f")(n / 1000) } label="log10(Bytes)">
                <Metric name="cr.node.sql.mon.client.max-max" title="Max Mem Usage (log10)"
                        aggregateMax downsampleMax />
                <Metric name="cr.node.sql.mon.client.max-p99" title="99th percentile max mem usage (log10)"
                        aggregateMax downsampleMax />
                <Metric name="cr.node.sql.mon.client.max-p90" title="90th percentile max mem usage (log10)"
                        aggregateMax downsampleMax />
                <Metric name="cr.node.sql.mon.client.max-p50" title="50th percentile max mem usage (log10)"
                        aggregateMax downsampleMax />
              </Axis>
            </LineGraph>

            <LineGraph title="SQL Session Cumulative Max Size (Admin)"
                       subtitle="(log10(Max) Per Percentile)"
                       tooltip={`The maximum memory usage per SQL admin session (including session-bound and txn-bound data), displayed as log(max).
                                 Percentiles are first calculated on each node.
                                 For each percentile, the maximum usage across all nodes is then shown.`}
                       sources={sources}>
              <Axis format={ (n: number) => d3.format(".3f")(n / 1000) } label="log10(Bytes)">
                <Metric name="cr.node.sql.mon.admin.max-max" title="Max Mem Usage (log10)"
                        aggregateMax downsampleMax />
                <Metric name="cr.node.sql.mon.admin.max-p99" title="99th percentile max mem usage (log10)"
                        aggregateMax downsampleMax />
                <Metric name="cr.node.sql.mon.admin.max-p90" title="90th percentile max mem usage (log10)"
                        aggregateMax downsampleMax />
                <Metric name="cr.node.sql.mon.admin.max-p50" title="50th percentile max mem usage (log10)"
                        aggregateMax downsampleMax />
              </Axis>
            </LineGraph>

            <LineGraph title="SQL Session Cumulative Max Size (Internal)"
                       subtitle="(log10(Max) Per Percentile)"
                       tooltip={`The maximum memory usage per SQL internal session (including session-bound and txn-bound data), displayed as log(max).
                                 Percentiles are first calculated on each node.
                                 For each percentile, the maximum usage across all nodes is then shown.`}
                       sources={sources}>
              <Axis format={ (n: number) => d3.format(".3f")(n / 1000) } label="log10(Bytes)">
                <Metric name="cr.node.sql.mon.internal.max-max" title="Max Mem Usage (log10)"
                        aggregateMax downsampleMax />
                <Metric name="cr.node.sql.mon.internal.max-p99" title="99th percentile max mem usage (log10)"
                        aggregateMax downsampleMax />
                <Metric name="cr.node.sql.mon.internal.max-p90" title="90th percentile max mem usage (log10)"
                        aggregateMax downsampleMax />
                <Metric name="cr.node.sql.mon.internal.max-p50" title="50th percentile max mem usage (log10)"
                        aggregateMax downsampleMax />
              </Axis>
            </LineGraph>

            <LineGraph title="SQL Txn Max Size"
                       subtitle="(log10(Max) Per Percentile)"
                       tooltip={`The maximum memory usage per SQL txn, displayed as log(max).
                                 Percentiles are first calculated on each node.
                                 For each percentile, the maximum usage across all nodes is then shown.`}
                       sources={sources}>
              <Axis format={ (n: number) => d3.format(".3f")(n / 1000) } label="log10(Bytes)">
                <Metric name="cr.node.sql.mon.client.txn.max-max" title="Max Mem Usage (log10)"
                        aggregateMax downsampleMax />
                <Metric name="cr.node.sql.mon.client.txn.max-p99" title="99th percentile max mem usage (log10)"
                        aggregateMax downsampleMax />
                <Metric name="cr.node.sql.mon.client.txn.max-p90" title="90th percentile max mem usage (log10)"
                        aggregateMax downsampleMax />
                <Metric name="cr.node.sql.mon.client.txn.max-p50" title="50th percentile max mem usage (log10)"
                        aggregateMax downsampleMax />
              </Axis>
            </LineGraph>

            <LineGraph title="SQL Txn Max Size (Admin)"
                       subtitle="(log10(Max) Per Percentile)"
                       tooltip={`The maximum memory usage per SQL admin session (including session-bound and txn-bound data), displayed as log(max).
                                 Percentiles are first calculated on each node.
                                 For each percentile, the maximum usage across all nodes is then shown.`}
                       sources={sources}>
              <Axis format={ (n: number) => d3.format(".3f")(n / 1000) } label="log10(Bytes)">
                <Metric name="cr.node.sql.mon.admin.txn.max-max" title="Max Mem Usage (log10)"
                        aggregateMax downsampleMax />
                <Metric name="cr.node.sql.mon.admin.txn.max-p99" title="99th percentile max mem usage (log10)"
                        aggregateMax downsampleMax />
                <Metric name="cr.node.sql.mon.admin.txn.max-p90" title="90th percentile max mem usage (log10)"
                        aggregateMax downsampleMax />
                <Metric name="cr.node.sql.mon.admin.txn.max-p50" title="50th percentile max mem usage (log10)"
                        aggregateMax downsampleMax />
              </Axis>
            </LineGraph>

            <LineGraph title="SQL Txn Max Size (Internal)"
                       subtitle="(log10(Max) Per Percentile)"
                       tooltip={`The maximum memory usage per SQL internal session (including session-bound and txn-bound data), displayed as log(max).
                                 Percentiles are first calculated on each node.
                                 For each percentile, the maximum usage across all nodes is then shown.`}
                       sources={sources}>
              <Axis format={ (n: number) => d3.format(".3f")(n / 1000) } label="log10(Bytes)">
                <Metric name="cr.node.sql.mon.internal.txn.max-max" title="Max Mem Usage (log10)"
                        aggregateMax downsampleMax />
                <Metric name="cr.node.sql.mon.internal.txn.max-p99" title="99th percentile max mem usage (log10)"
                        aggregateMax downsampleMax />
                <Metric name="cr.node.sql.mon.internal.txn.max-p90" title="90th percentile max mem usage (log10)"
                        aggregateMax downsampleMax />
                <Metric name="cr.node.sql.mon.internal.txn.max-p50" title="50th percentile max mem usage (log10)"
                        aggregateMax downsampleMax />
              </Axis>
            </LineGraph>

          </GraphGroup>
      </div>
    </div>;
  }
}
