// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { AxisUnits } from "@cockroachlabs/cluster-ui";
import map from "lodash/map";
import React from "react";

import LineGraph from "src/views/cluster/components/linegraph";
import {
  CapacityGraphTooltip,
  LiveBytesGraphTooltip,
} from "src/views/cluster/containers/nodeGraphs/dashboards/graphTooltips";
import { Metric, Axis } from "src/views/shared/components/metricQuery";

import {
  GraphDashboardProps,
  nodeDisplayName,
  storeIDsForNode,
} from "./dashboardUtils";
import { multipleStoreMetrics, storeMetrics } from "./storeUtils";

export default function (props: GraphDashboardProps) {
  const {
    nodeIDs,
    nodeSources,
    storeSources,
    tooltipSelection,
    storeIDsByNodeID,
    nodeDisplayNameByID,
    tenantSource,
  } = props;

  const getNodeNameById = (id: string) =>
    nodeDisplayName(nodeDisplayNameByID, id);

  return [
    <LineGraph
      title="Capacity"
      sources={storeSources}
      isKvGraph={true}
      tenantSource={tenantSource}
      tooltip={<CapacityGraphTooltip tooltipSelection={tooltipSelection} />}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Bytes} label="capacity">
        <Metric name="cr.store.capacity" title="Max" />
        <Metric name="cr.store.capacity.available" title="Available" />
        <Metric name="cr.store.capacity.used" title="Used" />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Live Bytes"
      isKvGraph={false}
      sources={storeSources}
      tenantSource={tenantSource}
      tooltip={<LiveBytesGraphTooltip tooltipSelection={tooltipSelection} />}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Bytes} label="live bytes">
        <Metric name="cr.store.livebytes" title="Live" />
        <Metric name="cr.store.sysbytes" title="System" />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="WAL Fsync Latency"
      sources={storeSources}
      isKvGraph={true}
      tenantSource={tenantSource}
      tooltip={`The latency for fsyncs to the storage engine's write-ahead log.`}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {map(nodeIDs, nid => (
          <>
            <Metric
              key={nid}
              name="cr.store.storage.wal.fsync.latency-p99.9"
              title={"p99.9 " + getNodeNameById(nid)}
              sources={storeIDsForNode(storeIDsByNodeID, nid)}
              aggregateMax
            />
            <Metric
              key={nid}
              name="cr.store.storage.wal.fsync.latency-p99.99"
              title={"p99.99 " + getNodeNameById(nid)}
              sources={storeIDsForNode(storeIDsByNodeID, nid)}
              aggregateMax
            />
            <Metric
              key={nid}
              name="cr.store.storage.wal.fsync.latency-max"
              title={"p100 " + getNodeNameById(nid)}
              sources={storeIDsForNode(storeIDsByNodeID, nid)}
              aggregateMax
            />
          </>
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Log Commit Latency"
      sources={storeSources}
      isKvGraph={true}
      tenantSource={tenantSource}
      tooltip={`The latency for commits to the Raft Log. This is typically
          dominated by the latency of an fdatasync to the storage engine's
          write-ahead log.`}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {multipleStoreMetrics(
          [
            {
              prefix: "p99.9",
              name: "cr.store.raft.process.logcommit.latency-p99.9",
              aggregateMax: true,
            },
            {
              prefix: "p99",
              name: "cr.store.raft.process.logcommit.latency-p99",
              aggregateMax: true,
            },
            {
              prefix: "p50",
              name: "cr.store.raft.process.logcommit.latency-p50",
              aggregateMax: true,
            },
          ],
          nodeIDs,
          storeIDsByNodeID,
        )}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Command Commit Latency"
      sources={storeSources}
      isKvGraph={true}
      tenantSource={tenantSource}
      tooltip={`The latency for commits of Raft commands. This measures
          applying a batch to the storage engine (including writes to the
          write-ahead log), but no fsync.`}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {multipleStoreMetrics(
          [
            {
              prefix: "p99",
              name: "cr.store.raft.process.commandcommit.latency-p99",
              aggregateMax: true,
            },
            {
              prefix: "p50",
              name: "cr.store.raft.process.commandcommit.latency-p50",
              aggregateMax: true,
            },
          ],
          nodeIDs,
          storeIDsByNodeID,
        )}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Read Amplification"
      sources={storeSources}
      isKvGraph={true}
      tenantSource={tenantSource}
      tooltip={`The average number of real read operations executed per logical read
          operation ${tooltipSelection}.`}
      showMetricsInTooltip={true}
    >
      <Axis label="factor">
        {storeMetrics(
          {
            name: "cr.store.rocksdb.read-amplification",
            aggregateAvg: true,
          },
          nodeIDs,
          storeIDsByNodeID,
        )}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="File Counts"
      sources={storeSources}
      isKvGraph={true}
      tenantSource={tenantSource}
      tooltip={`The number of files in use by type ${tooltipSelection}.`}
      showMetricsInTooltip={true}
    >
      <Axis label="files">
        {multipleStoreMetrics(
          [
            {
              prefix: "sstables",
              name: "cr.store.rocksdb.num-sstables",
              aggregateMax: true,
            },
            {
              prefix: "blob files",
              name: "cr.store.storage.value_separation.blob_files.count",
              aggregateMax: true,
            },
          ],
          nodeIDs,
          storeIDsByNodeID,
        )}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="L0 SSTable Count"
      sources={storeSources}
      isKvGraph={true}
      tenantSource={tenantSource}
      tooltip={`The number of L0 SSTables in use for each store ${tooltipSelection}.`}
      showMetricsInTooltip={true}
    >
      <Axis label="sstables">
        {storeMetrics(
          { name: "cr.store.storage.l0-num-files" },
          nodeIDs,
          storeIDsByNodeID,
        )}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="L0 SSTable Size"
      sources={storeSources}
      isKvGraph={true}
      tenantSource={tenantSource}
      tooltip={`The size of all L0 SSTables in use for each store ${tooltipSelection}.`}
      showMetricsInTooltip={true}
    >
      <Axis label="Size" units={AxisUnits.Bytes}>
        {storeMetrics(
          { name: "cr.store.storage.l0-level-size" },
          nodeIDs,
          storeIDsByNodeID,
        )}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Flushes"
      sources={storeSources}
      isKvGraph={true}
      tenantSource={tenantSource}
      tooltip={`Bytes written by memtable flushes ${tooltipSelection}.`}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Bytes} label="written bytes">
        {storeMetrics(
          {
            name: "cr.store.rocksdb.flushed-bytes",
            nonNegativeRate: true,
          },
          nodeIDs,
          storeIDsByNodeID,
        )}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="WAL Bytes Written"
      sources={storeSources}
      isKvGraph={true}
      tenantSource={tenantSource}
      tooltip={`Bytes written to WAL files ${tooltipSelection}.`}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Bytes} label="written bytes">
        {storeMetrics(
          {
            name: "cr.store.storage.wal.bytes_written",
            nonNegativeRate: true,
          },
          nodeIDs,
          storeIDsByNodeID,
        )}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Compactions"
      sources={storeSources}
      isKvGraph={true}
      tenantSource={tenantSource}
      tooltip={`Bytes written by compactions ${tooltipSelection}.`}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Bytes} label="written bytes">
        {storeMetrics(
          {
            name: "cr.store.rocksdb.compacted-bytes-written",
            nonNegativeRate: true,
          },
          nodeIDs,
          storeIDsByNodeID,
        )}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Ingestions"
      sources={storeSources}
      isKvGraph={true}
      tenantSource={tenantSource}
      tooltip={`Bytes written by sstable ingestions ${tooltipSelection}.`}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Bytes} label="written bytes">
        {storeMetrics(
          {
            name: "cr.store.rocksdb.ingested-bytes",
            nonNegativeRate: true,
          },
          nodeIDs,
          storeIDsByNodeID,
        )}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Write Stalls"
      sources={storeSources}
      isKvGraph={true}
      tenantSource={tenantSource}
      tooltip={`The number of intentional write stalls per second ${tooltipSelection}.
          Write stalls are used to backpressure incoming writes during periods
          of heavy write traffic.`}
      showMetricsInTooltip={true}
    >
      <Axis label="count">
        <Metric
          name="cr.store.storage.write-stalls"
          title="Write Stalls"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Disk Write Breakdown"
      sources={storeSources}
      isKvGraph={true}
      tenantSource={tenantSource}
      tooltip={
        <div>
          The number of bytes written to disk per second categorized according
          to the source {tooltipSelection}.
          <br />
          See the "Hardware" dashboard to view an aggregate of all disk writes.
        </div>
      }
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Bytes} label="bytes">
        {[
          "pebble-wal",
          "pebble-blob-file-rewrite",
          "pebble-compaction",
          "pebble-ingestion",
          "pebble-manifest",
          "pebble-memtable-flush",
          "raft-snapshot",
          "encryption-registry",
          "crdb-log",
          "sql-row-spill",
          "sql-col-spill",
        ].map(category =>
          map(nodeIDs, nid => (
            <Metric
              key={category + "-" + nid}
              name={`cr.store.storage.category-${category}.bytes-written`}
              title={category + "-" + getNodeNameById(nid)}
              sources={storeIDsForNode(storeIDsByNodeID, nid)}
              nonNegativeRate
            />
          )),
        )}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Store Disk Write Bytes/s"
      sources={storeSources}
      isKvGraph={true}
      tenantSource={tenantSource}
      tooltip={
        <div>
          The number of bytes written to the store's disk per second{" "}
          {tooltipSelection} (as reported by the OS).
        </div>
      }
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Bytes} label="bytes">
        {storeMetrics(
          {
            name: "cr.store.storage.disk.write.bytes",
            nonNegativeRate: true,
          },
          nodeIDs,
          storeIDsByNodeID,
        )}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Iterator Block Bytes"
      sources={storeSources}
      isKvGraph={true}
      tenantSource={tenantSource}
      tooltip={
        <div>
          The number of bytes of blocks loaded by iterators categorized
          according to the source {tooltipSelection}. These sums include blocks
          loaded from the block cache, blocks loaded from OS page cache and
          blocks loaded from disk.
          <br />
          See the "Hardware" dashboard to view an aggregate of all disk reads.
        </div>
      }
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Bytes} label="bytes">
        {multipleStoreMetrics(
          [
            "abort-span",
            "backup",
            "batch-eval",
            "crdb-unknown",
            "intent-resolution",
            "mvcc-gc",
            "pebble-compaction",
            "pebble-get",
            "pebble-ingest",
            "range-snap",
            "rangefeed",
            "replication",
            "scan-background",
            "scan-regular",
            "unknown",
          ].map(category => ({
            prefix: category,
            name:
              `cr.store.storage.iterator.category-` +
              category +
              `.block-load.bytes`,
            nonNegativeRate: true,
          })),
          nodeIDs,
          storeIDsByNodeID,
        )}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Store Disk Read Bytes/s"
      sources={storeSources}
      isKvGraph={true}
      tenantSource={tenantSource}
      tooltip={
        <div>
          The number of bytes read from the store's disk per second{" "}
          {tooltipSelection} (as reported by the OS).
        </div>
      }
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Bytes} label="bytes">
        {storeMetrics(
          {
            name: "cr.store.storage.disk.read.bytes",
            nonNegativeRate: true,
          },
          nodeIDs,
          storeIDsByNodeID,
        )}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Value Separated Bytes"
      sources={storeSources}
      isKvGraph={true}
      tenantSource={tenantSource}
      tooltip={
        <div>
          The volume of bytes stored separated from keys, externally in blob
          files.
        </div>
      }
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Bytes} label="bytes">
        {multipleStoreMetrics(
          [
            {
              prefix: "referenced",
              name: "cr.store.storage.value_separation.value_bytes.referenced",
              aggregateMax: true,
            },
            {
              prefix: "unreferenced",
              name: "cr.store.storage.value_separation.value_bytes.unreferenced",
              aggregateMax: true,
            },
          ],
          nodeIDs,
          storeIDsByNodeID,
        )}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Unreclaimed Disk Space"
      sources={storeSources}
      isKvGraph={true}
      tenantSource={tenantSource}
      tooltip={
        <div>
          The estimated volume of physical bytes that are obsolete and should
          eventually be reclaimed by compactions or other asynchronous
          processes.
        </div>
      }
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Bytes} label="bytes">
        {multipleStoreMetrics(
          [
            {
              prefix: "zombie sstables",
              name: "cr.store.storage.sstable.zombie.bytes",
              aggregateMax: true,
            },
            {
              prefix: "range deletions",
              name: "cr.store.storage.range_deletions.bytes",
              aggregateMax: true,
            },
            {
              prefix: "point deletions",
              name: "cr.store.storage.point_deletions.bytes",
              aggregateMax: true,
            },
          ],
          nodeIDs,
          storeIDsByNodeID,
        )}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Blob File Sizes"
      sources={storeSources}
      isKvGraph={true}
      tenantSource={tenantSource}
      tooltip={
        <div>
          The aggregate physical size of blob files storing separated values.
        </div>
      }
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Bytes} label="bytes">
        {storeMetrics(
          {
            name: "cr.store.storage.value_separation.blob_files.size",
          },
          nodeIDs,
          storeIDsByNodeID,
        )}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="File Descriptors"
      sources={nodeSources}
      isKvGraph={true}
      tenantSource={tenantSource}
      tooltip={`The number of open file descriptors ${tooltipSelection}, compared with
          the file descriptor limit.`}
      showMetricsInTooltip={true}
    >
      <Axis label="descriptors">
        <Metric name="cr.node.sys.fd.open" title="Open" />
        <Metric name="cr.node.sys.fd.softlimit" title="Limit" />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Time Series Writes"
      sources={nodeSources}
      isKvGraph={true}
      tenantSource={tenantSource}
      tooltip={`The number of successfully written time series samples, and number of
          errors attempting to write time series, per second ${tooltipSelection}.`}
      showMetricsInTooltip={true}
    >
      <Axis label="count">
        <Metric
          name="cr.node.timeseries.write.samples"
          title="Samples Written"
          nonNegativeRate
        />
        <Metric
          name="cr.node.timeseries.write.errors"
          title="Errors"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Time Series Bytes Written"
      sources={nodeSources}
      isKvGraph={true}
      tenantSource={tenantSource}
      tooltip={
        <div>
          The number of bytes written by the time series system per second{" "}
          {tooltipSelection}.
          <br />
          Note that this does not reflect the rate at which disk space is
          consumed by time series; the data is highly compressed on disk. This
          rate is instead intended to indicate the amount of network traffic and
          disk activity generated by time series writes.
          <br />
          See the "databases" tab to find the current disk usage for time series
          data.
        </div>
      }
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Bytes}>
        <Metric
          name="cr.node.timeseries.write.bytes"
          title="Bytes Written"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,
  ];
}
