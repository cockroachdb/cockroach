import d3 from "d3";
import React from "react";
import { connect } from "react-redux";
import { createSelector } from "reselect";

import { nodesSummarySelector, NodesSummary } from "src/redux/nodes";
import { Bytes as formatBytes } from "src/util/format";
import { NodesOverview } from "src/views/cluster/containers/nodesOverview";
import { CapacityChart } from "./capacity";

import "./cluster.styl";

class ClusterTicker extends React.Component<{}, {}> {
  render() {
    return (
      <section className="section cluster-ticker">
        <h2>Cluster Overview</h2>
      </section>
    );
  }
}

interface CapacityUsageProps {
  usedCapacity: number;
  usableCapacity: number;
}

const formatPercentage = d3.format("0.1%");

class CapacityUsage extends React.Component<CapacityUsageProps, {}> {
  render() {
    const { usedCapacity, usableCapacity } = this.props;
    const usedPercentage = usableCapacity !== 0 ? usedCapacity / usableCapacity : 0;
    return (
      <div className="cluster-summary__section capacity-usage">
        <h3 className="cluster-summary__section--title">Capacity Usage</h3>
        <div className="cluster-summary__section--details">
          <div className="cluster-summary__chart">
            <span className="value">{ formatPercentage(usedPercentage) }</span>
            <CapacityChart used={usedCapacity} usable={usableCapacity} />
          </div>
          <div className="cluster-summary__aside">
            <span className="label">Current Usage</span>
            <span className="value">{ formatBytes(usedCapacity) }</span>
          </div>
        </div>
      </div>
    );
  }
}

const mapStateToCapacityUsageProps = createSelector(
  nodesSummarySelector,
  function (nodesSummary: NodesSummary) {
    const { capacityAvailable, capacityUsed } = nodesSummary.nodeSums;
    const usableCapacity = capacityAvailable + capacityUsed;
    return {
      usedCapacity: capacityUsed,
      usableCapacity: usableCapacity,
    };
  },
);

// tslint:disable-next-line:variable-name
const CapacityUsageConnected = connect(mapStateToCapacityUsageProps)(CapacityUsage);

interface NodeLivenessProps {
  liveNodes: number;
  suspectNodes: number;
  deadNodes: number;
}

class NodeLiveness extends React.Component<NodeLivenessProps, {}> {
  render() {
    const { liveNodes, suspectNodes, deadNodes } = this.props;
    return (
      <div className="cluster-summary__section">
        <h3 className="cluster-summary__section--title">Node Liveness</h3>
        <div className="cluster-summary__section--metrics">
          <div className="cluster-summary__metric">
            <span className="label">Live<br />Nodes</span>
            <span className="value">{ liveNodes }</span>
          </div>
          <div className="cluster-summary__metric">
            <span className="label">Suspect<br />Nodes</span>
            <span className={ "value" + (suspectNodes ? " warning" : "") }>{ suspectNodes }</span>
          </div>
          <div className="cluster-summary__metric">
            <span className="label">Dead<br />Nodes</span>
            <span className={ "value" + (deadNodes ? " alert" : "") }>{ deadNodes }</span>
          </div>
        </div>
      </div>
    );
  }
}

const mapStateToNodeLivenessProps = createSelector(
  nodesSummarySelector,
  function (nodesSummary: NodesSummary) {
    const { nodeCounts } = nodesSummary.nodeSums;
    return {
      liveNodes: nodeCounts.healthy,
      suspectNodes: nodeCounts.suspect,
      deadNodes: nodeCounts.dead,
    };
  },
);

// tslint:disable-next-line:variable-name
const NodeLivenessConnected = connect(mapStateToNodeLivenessProps)(NodeLiveness);

interface ReplicationStatusProps {
  totalRanges: number;
  underReplicatedRanges: number;
  unavailableRanges: number;
}

class ReplicationStatus extends React.Component<ReplicationStatusProps, {}> {
  render() {
    const { totalRanges, underReplicatedRanges, unavailableRanges } = this.props;
    return (
      <div className="cluster-summary__section">
        <h3 className="cluster-summary__section--title">Replication Status</h3>
        <div className="cluster-summary__section--metrics">
          <div className="cluster-summary__metric">
            <span className="label">Total<br />Ranges</span>
            <span className="value">{ totalRanges }</span>
          </div>
          <div className="cluster-summary__metric">
            <span className="label">Under-replicated<br />Ranges</span>
            <span className={ "value" + (underReplicatedRanges ? " warning" : "") }>{ underReplicatedRanges }</span>
          </div>
          <div className="cluster-summary__metric">
            <span className="label">Unavailable<br />Ranges</span>
            <span className={ "value" + (unavailableRanges ? " alert" : "") }>{ unavailableRanges }</span>
          </div>
        </div>
      </div>
    );
  }
}

const mapStateToReplicationStatusProps = createSelector(
  nodesSummarySelector,
  function (nodesSummary: NodesSummary) {
    const { totalRanges, underReplicatedRanges, unavailableRanges } = nodesSummary.nodeSums;
    return {
      totalRanges: totalRanges,
      underReplicatedRanges: underReplicatedRanges,
      unavailableRanges: unavailableRanges,
    };
  },
);

// tslint:disable-next-line:variable-name
const ReplicationStatusConnected = connect(mapStateToReplicationStatusProps)(ReplicationStatus);

class ClusterSummary extends React.Component<{}, {}> {
  render() {
    return (
      <section className="section cluster-summary">
        <CapacityUsageConnected />
        <NodeLivenessConnected />
        <ReplicationStatusConnected />
      </section>
    );
  }
}

/**
 * Renders the main content of the cluster visualization page.
 */
class ClusterOverview extends React.Component<{}, {}> {
  render() {
    return (
      <div>
        <div className="cluster-overview">
          <ClusterTicker />
          <ClusterSummary />
        </div>
        <NodesOverview />
      </div>
    );
  }
}

export { ClusterOverview as default };
