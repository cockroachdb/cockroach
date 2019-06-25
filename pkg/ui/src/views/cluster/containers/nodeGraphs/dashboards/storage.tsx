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
import { Metric, Axis, AxisUnits } from "src/views/shared/components/metricQuery";

import { GraphDashboardProps, nodeDisplayName, storeIDsForNode } from "./dashboardUtils";

export default function (props: GraphDashboardProps) {
  const { nodeIDs, nodesSummary, nodeSources, storeSources, tooltipSelection } = props;

  return [
    <LineGraph
      title="Capacity"
      sources={storeSources}
      tooltip={`Summary of total and available capacity ${tooltipSelection}.`}
    >
      <Axis units={AxisUnits.Bytes} label="capacity">
        <Metric name="cr.store.capacity" title="Capacity" />
        <Metric name="cr.store.capacity.available" title="Available" />
        <Metric name="cr.store.capacity.used" title="Used" />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Live Bytes"
      sources={storeSources}
      tooltip={
        `The amount of Live data used by both applications and the
           CockroachDB system ${tooltipSelection}. This excludes historical and deleted data.`
      }
    >
      <Axis units={AxisUnits.Bytes} label="live bytes">
        <Metric name="cr.store.livebytes" title="Live" />
        <Metric name="cr.store.sysbytes" title="System" />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Log Commit Latency: 99th Percentile"
      sources={storeSources}
      tooltip={`The 99th %ile latency for commits to the Raft Log.
        This measures essentially an fdatasync to the storage engine's write-ahead log.`}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {
          _.map(nodeIDs, (nid) => (
            <Metric
              key={nid}
              name="cr.store.raft.process.logcommit.latency-p99"
              title={nodeDisplayName(nodesSummary, nid)}
              sources={storeIDsForNode(nodesSummary, nid)}
            />
          ))
        }
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Log Commit Latency: 50th Percentile"
      sources={storeSources}
      tooltip={`The 50th %ile latency for commits to the Raft Log.
        This measures essentially an fdatasync to the storage engine's write-ahead log.`}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {
          _.map(nodeIDs, (nid) => (
            <Metric
              key={nid}
              name="cr.store.raft.process.logcommit.latency-p50"
              title={nodeDisplayName(nodesSummary, nid)}
              sources={storeIDsForNode(nodesSummary, nid)}
            />
          ))
        }
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Command Commit Latency: 99th Percentile"
      sources={storeSources}
      tooltip={`The 99th %ile latency for commits of Raft commands.
        This measures applying a batch to the storage engine
        (including writes to the write-ahead log), but no fsync.`}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {
          _.map(nodeIDs, (nid) => (
            <Metric
              key={nid}
              name="cr.store.raft.process.commandcommit.latency-p99"
              title={nodeDisplayName(nodesSummary, nid)}
              sources={storeIDsForNode(nodesSummary, nid)}
            />
          ))
        }
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Command Commit Latency: 50th Percentile"
      sources={storeSources}
      tooltip={`The 50th %ile latency for commits of Raft commands.
        This measures applying a batch to the storage engine
        (including writes to the write-ahead log), but no fsync.`}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {
          _.map(nodeIDs, (nid) => (
            <Metric
              key={nid}
              name="cr.store.raft.process.commandcommit.latency-p50"
              title={nodeDisplayName(nodesSummary, nid)}
              sources={storeIDsForNode(nodesSummary, nid)}
            />
          ))
        }
      </Axis>
    </LineGraph>,

    <LineGraph
      title="RocksDB Read Amplification"
      sources={storeSources}
      tooltip={
        `RocksDB read amplification statistic; measures the average number of real read operations
           executed per logical read operation ${tooltipSelection}.`
      }
    >
      <Axis label="factor">
        <Metric name="cr.store.rocksdb.read-amplification" title="Read Amplification" aggregateAvg />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="RocksDB SSTables"
      sources={storeSources}
      tooltip={`The number of RocksDB SSTables in use ${tooltipSelection}.`}
    >
      <Axis label="sstables">
        <Metric name="cr.store.rocksdb.num-sstables" title="SSTables" />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="File Descriptors"
      sources={nodeSources}
      tooltip={
        `The number of open file descriptors ${tooltipSelection}, compared with the
          file descriptor limit.`
      }
    >
      <Axis label="descriptors">
        <Metric name="cr.node.sys.fd.open" title="Open" />
        <Metric name="cr.node.sys.fd.softlimit" title="Limit" />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="RocksDB Compactions/Flushes"
      sources={storeSources}
      tooltip={
        `The number of RocksDB compactions and memtable flushes, per second ${tooltipSelection}.`
      }
    >
      <Axis label="count">
        <Metric name="cr.store.rocksdb.compactions" title="Compactions" nonNegativeRate />
        <Metric name="cr.store.rocksdb.flushes" title="Flushes" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Time Series Writes"
      sources={nodeSources}
      tooltip={
        `The number of successfully written time series samples, and number of errors attempting
        to write time series, per second ${tooltipSelection}.`
      }
    >
      <Axis label="count">
        <Metric name="cr.node.timeseries.write.samples" title="Samples Written" nonNegativeRate />
        <Metric name="cr.node.timeseries.write.errors" title="Errors" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Time Series Bytes Written"
      sources={nodeSources}
      tooltip={
        <div>
          The number of bytes written by the time series system per second {tooltipSelection}.
          <br />
          Note that this does not reflect the rate at which disk space is consumed by time series;
          the data is highly compressed on disk. This rate is instead intended to indicate the
          amount of network traffic and disk activity generated by time series writes.
          <br />
          See the "databases" tab to find the current disk usage for time series data.
        </div>
      }
    >
      <Axis units={AxisUnits.Bytes}>
        <Metric name="cr.node.timeseries.write.bytes" title="Bytes Written" nonNegativeRate />
      </Axis>
    </LineGraph>,
  ];
}
