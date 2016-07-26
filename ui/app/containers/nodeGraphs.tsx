import * as React from "react";
import * as d3 from "d3";
import { IInjectedProps } from "react-router";

import { nodeID } from "./../util/constants";

import GraphGroup from "../components/graphGroup";
import { LineGraph, Axis, Metric } from "../components/linegraph";
import { StackedAreaGraph } from "../components/stackedgraph";
import { Bytes } from "../util/format";
import { NanoToMilli } from "../util/convert";

/**
 * Renders the main content of the help us page.
 */
export default class extends React.Component<IInjectedProps, {}> {
  static displayTimeScale = true;

  render() {
    let sources: string[];
    let node = this.props.params[nodeID];
    sources = node ? [node] : null;
    let specifier = node ? `on node ${node}` : "across all nodes";

    return <div className="section node">
      <div className="charts">
        <h2>Activity</h2>
          <GraphGroup groupId="node.activity">
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
                <Metric name="cr.node.exec.latency-1m-max" title="Max Latency"
                        aggregateMax downsampleMax />
                <Metric name="cr.node.exec.latency-1m-p99" title="99th percentile latency"
                        aggregateMax downsampleMax />
                <Metric name="cr.node.exec.latency-1m-p90" title="90th percentile latency"
                        aggregateMax downsampleMax />
                <Metric name="cr.node.exec.latency-1m-p50" title="50th percentile latency"
                        aggregateMax downsampleMax />
              </Axis>
            </LineGraph>

            <LineGraph title="GC Pause Time" sources={sources} tooltip={`The ${sources ? "average and maximum" : ""} amount of processor time used by Go’s garbage collector per second ${specifier}. During garbage collection, application code execution is paused.`}>
              <Axis label="Milliseconds" format={ (n) => d3.format(".1f")(NanoToMilli(n)) }>
                <Metric name="cr.node.sys.gc.pause.ns" title={`${sources ? "" : "Avg "}Time`} aggregateAvg nonNegativeRate />
                { node ? null : <Metric name="cr.node.sys.gc.pause.ns" title="Max Time" aggregateMax nonNegativeRate /> }
              </Axis>
            </LineGraph>

          </GraphGroup>
        <h2>SQL Queries</h2>
          <GraphGroup groupId="node.queries">
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
        <h2>System Resources</h2>
          <GraphGroup groupId="node.resources">

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
        <h2>Advanced Internals</h2>
          <GraphGroup groupId="node.internals">

            <StackedAreaGraph title="Key/Value Transactions" sources={sources}>
              <Axis label="transactions/sec" format={ d3.format(".1f") }>
                <Metric name="cr.node.txn.commits-count" title="Commits" nonNegativeRate />
                <Metric name="cr.node.txn.commits1PC-count" title="Fast 1PC" nonNegativeRate />
                <Metric name="cr.node.txn.aborts-count" title="Aborts" nonNegativeRate />
                <Metric name="cr.node.txn.abandons-count" title="Abandons" nonNegativeRate />
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
          </GraphGroup>
      </div>
    </div>;
  }
}
