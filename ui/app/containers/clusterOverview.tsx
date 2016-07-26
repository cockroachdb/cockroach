import * as React from "react";
import * as d3 from "d3";
import _ = require("lodash");
import { connect } from "react-redux";
import { createSelector } from "reselect";

import { AdminUIState } from "../redux/state";
import { refreshNodes } from "../redux/apiReducers";
import { LineGraph, Axis, Metric } from "../components/linegraph";
import { StackedAreaGraph } from "../components/stackedgraph";
import GraphGroup from "../components/graphGroup";
import { Bytes } from "../util/format";
import { NanoToMilli } from "../util/convert";
import { NodeStatus, MetricConstants, BytesUsed } from  "../util/proto";
import Visualization from "../components/visualization";

interface ClusterMainProps {
  clusterInfo: {
    totalNodes: number;
    availableCapacity: number;
    bytesUsed: number;
  };
  refreshNodes: typeof refreshNodes;
}

/**
 * ClusterMain renders the main content of the cluster page.
 */
class ClusterMain extends React.Component<ClusterMainProps, {}> {
  static displayTimeScale = true;

  static title() {
    return <h2>Cluster</h2>;
  }

  componentWillMount() {
    // Refresh nodes status query when mounting.
    this.props.refreshNodes();
  }

  componentWillReceiveProps(props: ClusterMainProps) {
    // Refresh nodes status query when props are received; this will immediately
    // trigger a new request if previous results are invalidated.
    props.refreshNodes();
  }

  render() {
    let { totalNodes, bytesUsed, availableCapacity } = this.props.clusterInfo;
    let capacityPercent = (availableCapacity !== 0) ? bytesUsed / (bytesUsed + availableCapacity) : 0.0;
    return <div className="section overview">
      <div className="charts half">

        <div style={{float:"left"}} className="small half">
          <Visualization
            title={ (totalNodes === 1) ? "Node" : "Nodes" }
            tooltip="The total number of nodes in the cluster."
            warning={ totalNodes < 3 ? <div>This cluster needs at least 3 nodes to tolerate failure.
              <br />See the <a href="https://www.cockroachlabs.com/docs/configure-replication-zones.html" target="_blank">
                docs
              </a> for more details.</div> : ""
            }
            warningTitle = { totalNodes < 3 ? "Low Replication" : "" }>
            <div className="visualization">
              <div style={{zoom:"100%"}} className="number">{ d3.format("s")(totalNodes) }</div>
            </div>
          </Visualization>
        </div>

        <div style={{float:"left"}} className="small half">
          <Visualization title="Capacity Used"
                         tooltip={`You are using ${Bytes(bytesUsed)} of ${Bytes(availableCapacity)} storage
                                   capacity across all nodes.`}>
            <div className="visualization">
              <div style={{zoom:"50%"}} className="number">{ d3.format("0.1%")(capacityPercent) }</div>
            </div>
          </Visualization>
        </div>

        <GraphGroup groupId="cluster.small" childClassName="small half">
          <LineGraph title="Query Time"
                     subtitle="(Max Per Percentile)"
                     tooltip={`The latency between query requests and responses over a 1 minute period.
                               Percentiles are first calculated on each node.
                               For each percentile, the maximum latency across all nodes is then shown.`}>
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

          <StackedAreaGraph title="CPU Usage"
                     legend={ false }
                     tooltip={`The average percentage of CPU used by CockroachDB (User %) and system-level operations
                               (Sys %) across all nodes.`}>
            <Axis format={ d3.format(".2%") }>
              <Metric name="cr.node.sys.cpu.user.percent" aggregateAvg title="CPU User %" />
              <Metric name="cr.node.sys.cpu.sys.percent" aggregateAvg title="CPU Sys %" />
            </Axis>
          </StackedAreaGraph>

          <LineGraph title="Memory Usage"
                     tooltip="The total memory in use across all nodes.">
            <Axis format={ Bytes }>
              <Metric name="cr.node.sys.rss" title="Memory" />
            </Axis>
          </LineGraph>
        </GraphGroup>
      </div>
      <div className="charts">
        <GraphGroup groupId="cluster.big">

          <LineGraph title="SQL Connections"
                     tooltip="The total number of active SQL connections across all nodes.">
            <Axis format={ d3.format(".1f") }>
              <Metric name="cr.node.sql.conns" title="Connections" />
            </Axis>
          </LineGraph>

          <LineGraph title="SQL Traffic"
                     tooltip="The average amount of SQL client network traffic in bytes per second across all nodes.">
            <Axis format={ Bytes }>
              <Metric name="cr.node.sql.bytesin" title="Bytes In" nonNegativeRate />
              <Metric name="cr.node.sql.inserts" title="Bytes Out" nonNegativeRate />
            </Axis>
          </LineGraph>

          <LineGraph title="Reads"
                     tooltip="The average number of SELECT statements per second across all nodes.">
            <Axis format={ d3.format(".1f") }>
              <Metric name="cr.node.sql.select.count" title="Selects" nonNegativeRate />
            </Axis>
          </LineGraph>

          <LineGraph title="Writes"
                     tooltip="The average number of INSERT, UPDATE, and DELETE statements per second across all nodes.">
            <Axis format={ d3.format(".1f") }>
              <Metric name="cr.node.sql.insert.count" title="Insert" nonNegativeRate />
              <Metric name="cr.node.sql.update.count" title="Update" nonNegativeRate />
              <Metric name="cr.node.sql.delete.count" title="Delete" nonNegativeRate />
            </Axis>
          </LineGraph>

        </GraphGroup>
      </div>
    </div>;
  }
}

let nodeStatuses = (state: AdminUIState): NodeStatus[] => state.cachedData.nodes.data;
let clusterInfo = createSelector(
  nodeStatuses,
  (nss) => {
    return {
      totalNodes: nss && nss.length || 0,
      availableCapacity: _.sumBy(nss, (ns) => ns.metrics.get(MetricConstants.availableCapacity)),
      bytesUsed: _.sumBy(nss, BytesUsed),
    };
  }
);

let clusterMainConnected = connect(
  (state: AdminUIState) => {
    return {
      clusterInfo: clusterInfo(state),
    };
  },
  {
      refreshNodes,
  }
)(ClusterMain);

export default clusterMainConnected;
