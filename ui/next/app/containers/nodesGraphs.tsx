/// <reference path="../../typings/main.d.ts" />
import * as React from "react";
import * as d3 from "d3";

import GraphGroup from "../components/graphGroup";
import { LineGraph, Axis, Metric } from "../components/linegraph";
import { StackedAreaGraph } from "../components/stackedgraph";
import { Bytes } from "../util/format";
import { NanoToMilli } from "../util/convert";

/**
 * Renders the graphs tab of the nodes page.
 */
export default class extends React.Component<{}, {}> {
  render() {
    return <div className="section nodes">
      <div className="charts">
        <h2>Activity</h2>
          <GraphGroup groupId="nodes.activity">

            <LineGraph title="SQL Connections">
              <Axis format={ d3.format(".1") }>
                <Metric name="cr.node.sql.conns" title="Client Connections" />
              </Axis>
            </LineGraph>

            <LineGraph title="SQL Traffic">
              <Axis format={ Bytes }>
                <Metric name="cr.node.sql.bytesin" title="Bytes In" nonNegativeRate />
                <Metric name="cr.node.sql.bytesout" title="Bytes Out" nonNegativeRate />
              </Axis>
            </LineGraph>

            <LineGraph title="Queries Per Second">
              <Axis format={ d3.format(".1") }>
                <Metric name="cr.node.sql.query.count" title="Queries/Sec" nonNegativeRate />
              </Axis>
            </LineGraph>

            <LineGraph title="Live Bytes">
              <Axis format={ Bytes }>
                <Metric name="cr.store.livebytes" title="Live Bytes" />
              </Axis>
            </LineGraph>

            <LineGraph title="Query Time"
                       subtitle="(Max Per Percentile)"
                       tooltip={`The latency between query requests and responses over a 1 minute period.
                                 Percentiles are first calculated on each node.
                                 For Each percentile, the maximum latency across all nodes is then shown.`}>
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

          </GraphGroup>
        <h2>SQL Queries</h2>
          <GraphGroup groupId="nodes.queries">

            <LineGraph title="Reads">
              <Axis format={ d3.format(".1") }>
                <Metric name="cr.node.sql.select.count" title="Selects" nonNegativeRate />
              </Axis>
            </LineGraph>

            <LineGraph title="Writes">
              <Axis format={ d3.format(".1") }>
                <Metric name="cr.node.sql.update.count" title="Updates" nonNegativeRate />
                <Metric name="cr.node.sql.insert.count" title="Inserts" nonNegativeRate />
                <Metric name="cr.node.sql.delete.count" title="Deletes" nonNegativeRate />
              </Axis>
            </LineGraph>

            <LineGraph title="Transactions">
              <Axis format={ d3.format(".1") }>
                <Metric name="cr.node.sql.txn.commit.count" title="Commits" nonNegativeRate />
                <Metric name="cr.node.sql.txn.rollback.count" title="Rollbacks" nonNegativeRate />
                <Metric name="cr.node.sql.txn.abort.count" title="Aborts" nonNegativeRate />
              </Axis>
            </LineGraph>

            <LineGraph title="Schema Changes">
              <Axis format={ d3.format(".1") }>
                <Metric name="cr.node.sql.ddl.count" title="DDL Statements" nonNegativeRate />
              </Axis>
            </LineGraph>

          </GraphGroup>
        <h2>System Resources</h2>
          <GraphGroup groupId="nodes.resources">

            <StackedAreaGraph title="CPU Usage">
              <Axis format={ d3.format(".2%") }>
                <Metric name="cr.node.sys.cpu.user.percent" title="CPU User %"/>
                <Metric name="cr.node.sys.cpu.sys.percent" title="CPU Sys %"/>
              </Axis>
            </StackedAreaGraph>

            <LineGraph title="Memory Usage">
              <Axis format={ Bytes }>
                <Metric name="cr.node.sys.allocbytes" title="Go In Use" />
                <Metric name="cr.node.sys.sysbytes" title="Go Sys" />
                <Metric name="cr.node.sys.rss" title="RSS" />
              </Axis>
            </LineGraph>

            <LineGraph title="Goroutine Count">
              <Axis format={ d3.format(".1") }>
                <Metric name="cr.node.sys.goroutines" title="Goroutine Count" />
              </Axis>
            </LineGraph>

            <LineGraph title="CGo Calls">
              <Axis format={ d3.format(".1") }>
                <Metric name="cr.node.sys.cgocalls" title="CGo Calls" />
              </Axis>
            </LineGraph>

          </GraphGroup>
        <h2>Internals</h2>
          <GraphGroup groupId="nodes.internals">

            <StackedAreaGraph title="Key/Value Transactions">
              <Axis label="transactions/sec" format={ d3.format(".1f") }>
                <Metric name="cr.node.txn.commits-count" title="Commits" nonNegativeRate />
                <Metric name="cr.node.txn.commits1PC-count" title="Fast 1PC" nonNegativeRate />
                <Metric name="cr.node.txn.aborts-count" title="Aborts" nonNegativeRate />
                <Metric name="cr.node.txn.abandons-count" title="Abandons" nonNegativeRate />
              </Axis>
            </StackedAreaGraph>

            <LineGraph title="Engine Memory Usage">
              <Axis format={ Bytes }>
                <Metric name="cr.store.rocksdb.block.cache.usage" title="Block Cache" />
                <Metric name="cr.store.rocksdb.block.cache.pinned-usage" title="Iterators" />
                <Metric name="cr.store.rocksdb.memtable.total-size" title="Memtable" />
              </Axis>
            </LineGraph>

            <StackedAreaGraph title="Block Cache Hits/Misses">
              <Axis format={ d3.format(".1") }>
                <Metric name="cr.store.rocksdb.block.cache.hits"
                        title="Cache Hits"
                        nonNegativeRate />
                <Metric name="cr.store.rocksdb.block.cache.misses"
                        title="Cache Missses"
                        nonNegativeRate />
              </Axis>
            </StackedAreaGraph>

            <StackedAreaGraph title="Range Events">
              <Axis format={ d3.format(".1f") }>
                <Metric name="cr.store.range.splits" title="Splits" nonNegativeRate />
                <Metric name="cr.store.range.adds" title="Adds" nonNegativeRate />
                <Metric name="cr.store.range.removes" title="Removes" nonNegativeRate />
              </Axis>
            </StackedAreaGraph>

            <LineGraph title="Flushes and Compactions">
              <Axis format={ d3.format(".1") }>
                <Metric name="cr.store.rocksdb.flushes" title="Flushes" nonNegativeRate />
                <Metric name="cr.store.rocksdb.compactions" title="Compactions" nonNegativeRate />
              </Axis>
            </LineGraph>

            <LineGraph title="Bloom Filter Prefix">
              <Axis format={ d3.format(".1") }>
                <Metric name="cr.store.rocksdb.bloom.filter.prefix.checked"
                        title="Checked"
                        nonNegativeRate />
                <Metric name="cr.store.rocksdb.bloom.filter.prefix.useful"
                        title="Useful"
                        nonNegativeRate />
              </Axis>
            </LineGraph>

            <LineGraph title="Clock Offset">
              <Axis label="Milliseconds" format={ (n) => d3.format(".1f")(NanoToMilli(n)) }>
                <Metric name="cr.node.clock-offset.upper-bound-nanos" title="Upper Bound" />
                <Metric name="cr.node.clock-offset.lower-bound-nanos" title="Lower Bound" />
              </Axis>
            </LineGraph>

            <LineGraph title="GC Pause Time">
              <Axis label="Milliseconds" format={ (n) => d3.format(".1f")(NanoToMilli(n)) }>
                <Metric name="cr.node.sys.gc.pause.ns" title="Time" nonNegativeRate />
              </Axis>
            </LineGraph>

          </GraphGroup>
      </div>
    </div>;
  }
}
