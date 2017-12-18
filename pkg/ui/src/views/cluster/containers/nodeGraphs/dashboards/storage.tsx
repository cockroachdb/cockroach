import React from "react";
import _ from "lodash";

import { LineGraph } from "src/views/cluster/components/linegraph";
import { Metric, Axis, AxisUnits } from "src/views/shared/components/metricQuery";

import { GraphDashboardProps, nodeAddress, storeIDsForNode } from "./dashboardUtils";

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
      tooltip={`The 99th %ile latency for commits to the Raft Log.`}
    >
      <Axis units={AxisUnits.Duration} label="latency">
        {
          _.map(nodeIDs, (nid) => (
            <Metric
              key={nid}
              name="cr.store.raft.process.logcommit.latency-p99"
              title={nodeAddress(nodesSummary, nid)}
              sources={storeIDsForNode(nodesSummary, nid)}
            />
          ))
        }
      </Axis>
    </LineGraph>,

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
              title={nodeAddress(nodesSummary, nid)}
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
  ];
}
