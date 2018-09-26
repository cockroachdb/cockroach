import React from "react";
import _ from "lodash";

import { AggregationLevel } from "src/redux/aggregationLevel";
import * as docsURL from "src/util/docs";
import { LineGraph } from "src/views/cluster/components/linegraph";
import { Metric, Axis, AxisUnits } from "src/views/shared/components/metricQuery";

import { GraphDashboardProps, nodeDisplayName, storeIDsForNode } from "./dashboardUtils";

export default function (props: GraphDashboardProps) {
  const { nodeIDs, nodesSummary, nodeSources, storeSources, tooltipSelection, aggregationLevel } = props;

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

  if (props.aggregationLevel === AggregationLevel.Cluster) {
    charts.push(
      <LineGraph
        title="Capacity"
        sources={storeSources}
        tooltip={(
          <div>
            <dl>
              <dt>Capacity</dt>
              <dd>
                Total disk space available {tooltipSelection} to CockroachDB.
                {" "}
                <em>
                  Control this value per node with the
                  {" "}
                  <code>
                    <a href={docsURL.startFlags} target="_blank">
                      --store
                    </a>
                  </code>
                  {" "}
                  flag.
                </em>
              </dd>
              <dt>Available</dt>
              <dd>Free disk space available {tooltipSelection} to CockroachDB.</dd>
              <dt>Used</dt>
              <dd>Disk space used {tooltipSelection} by CockroachDB.</dd>
            </dl>
          </div>
        )}
      >
        <Axis units={AxisUnits.Bytes} label="capacity">
          <Metric name="cr.store.capacity" title="Capacity" />
          <Metric name="cr.store.capacity.available" title="Available" />
          <Metric name="cr.store.capacity.used" title="Used" />
        </Axis>
      </LineGraph>,
    );
  } else {
    charts.push(
      <LineGraph
        title="Capacity"
        subtitle="Available"
        tooltip="Free disk space available to CockroachDB on each node."
      >
        <Axis units={AxisUnits.Bytes} label="capacity">
          {
            _.map(nodeIDs, (nid) => (
              <Metric
                key={nid}
                name="cr.store.capacity.available"
                title={nodeDisplayName(nodesSummary, nid)}
                sources={storeIDsForNode(nodesSummary, nid)}
              />
            ))
          }
        </Axis>
      </LineGraph>,
    );
  }

  charts.push(
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
  );

  charts.push(
    <LineGraph
      title="Log Commit Latency: 99th Percentile"
      sources={storeSources}
      tooltip={`The 99th %ile latency for commits to the Raft Log.`}
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
  );

  charts.push(
    <LineGraph
      title="Command Commit Latency: 99th Percentile"
      sources={storeSources}
      tooltip={`The 99th %ile latency for commits of Raft commands.`}
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
  );

  charts.push(
    <LineGraph
      title="RocksDB Read Amplification"
      sources={storeSources}
      tooltip={
        `RocksDB read amplification statistic; measures the average number of real read operations
           executed per logical read operation ${tooltipSelection}.`
      }
    >
      <Axis label="factor">
        {default_aggregate("cr.store.rocksdb.read-amplification", "Read Amplification", { aggregateAvg: true })}
      </Axis>
    </LineGraph>,
  );

  charts.push(
    <LineGraph
      title="RocksDB SSTables"
      sources={storeSources}
      tooltip={`The number of RocksDB SSTables in use ${tooltipSelection}.`}
    >
      <Axis label="sstables">
        {default_aggregate("cr.store.rocksdb.num-sstables", "SSTables")}
      </Axis>
    </LineGraph>,
  );

  charts.push(
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
  );

  if (props.aggregationLevel === AggregationLevel.Cluster) {
    charts.push(
      <LineGraph
        title="RocksDB Compactions/Flushes"
        sources={storeSources}
        tooltip={
          `The number of RocksDB compactions and memtable flushes per second ${tooltipSelection}.`
        }
      >
        <Axis label="count">
          <Metric name="cr.store.rocksdb.compactions" title="Compactions" nonNegativeRate />
          <Metric name="cr.store.rocksdb.flushes" title="Flushes" nonNegativeRate />
        </Axis>
      </LineGraph>,
    );
  } else {
    charts.push(
      <LineGraph
        title="RocksDB"
        subtitle="Compactions"
        tooltip={
          `The number of RocksDB compactions and memtable flushes per second on each node.`
        }
      >
        <Axis label="count">
          {nodeIDs.map((nid) => (
            <Metric
              name="cr.store.rocksdb.compactions"
              title={nodeDisplayName(nodesSummary, nid)}
              sources={storeIDsForNode(nodesSummary, nid)}
              nonNegativeRate
            />
          ))}
        </Axis>
      </LineGraph>,
    );
    charts.push(
      <LineGraph
        title="RocksDB"
        subtitle="Flushes"
      >
        <Axis label="count">
          {nodeIDs.map((nid) => (
            <Metric
              name="cr.store.rocksdb.flushes"
              title={nodeDisplayName(nodesSummary, nid)}
              sources={storeIDsForNode(nodesSummary, nid)}
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
    );
  } else {
    charts.push(
      <LineGraph
        title="Time Series"
        subtitle="Writes"
        sources={nodeSources}
        tooltip={
          `The number of successfully written time series samples, and number of errors attempting
          to write time series, per second ${tooltipSelection}.`
        }
      >
        <Axis label="count">
          {nodeIDs.map((nid) => (
            <Metric
              name="cr.node.timeseries.write.samples"
              title={nodeDisplayName(nodesSummary, nid)}
              sources={[nid]}
              nonNegativeRate
            />
          ))}
        </Axis>
      </LineGraph>,
    );
    charts.push(
      <LineGraph
        title="Time Series"
        subtitle="Errors"
        sources={nodeSources}
      >
        <Axis label="count">
          {nodeIDs.map((nid) => (
            <Metric
              name="cr.node.timeseries.write.errors"
              title={nodeDisplayName(nodesSummary, nid)}
              sources={[nid]}
              nonNegativeRate
            />
          ))}
        </Axis>
      </LineGraph>,
    );
  }

  charts.push(
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
        {default_aggregate("cr.node.timeseries.write.bytes", "Bytes Written", { nonNegativeRate: true })}
      </Axis>
    </LineGraph>,
  );

  return charts;
}
