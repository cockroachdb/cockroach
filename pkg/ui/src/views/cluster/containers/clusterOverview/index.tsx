import d3 from "d3";
import React from "react";
import { connect } from "react-redux";

import { AdminUIState } from "src/redux/state";
import { NodesOverview } from "src/views/cluster/containers/nodesOverview";

class ClusterTicker extends React.Component<{}, {}> {
  render() {
    return (
      <section className="section">
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
// TODO(couchand): Actual byte formatter.
const formatBytes = d3.format("0.2f");

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
            <span>BAR CHART HERE</span>
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

function mapStateToCapacityUsageProps(_state: AdminUIState) {
  return {
    usedCapacity: 2500000000,
    usableCapacity: 500000000000,
  };
}

// tslint:disable-next-line:variable-name
const CapacityUsageConnected = connect(mapStateToCapacityUsageProps)(CapacityUsage);

interface NodeLivenessProps {
  liveNodes: number;
  suspectNodes: number;
  deadNodes: number;
}

class NodeLiveness extends React.Component<NodeLivenessProps, {}> {
  render() {
    return (
      <div className="cluster-summary__section">
        <h3 className="cluster-summary__section--title">Node Liveness</h3>
        <div className="cluster-summary__section--metrics">
          <div className="cluster-summary__metric">
            <span className="label">Live<br />Nodes</span>
            <span className="value">{ this.props.liveNodes }</span>
          </div>
          <div className="cluster-summary__metric">
            <span className="label">Suspect<br />Nodes</span>
            <span className="value">{ this.props.suspectNodes }</span>
          </div>
          <div className="cluster-summary__metric">
            <span className="label">Dead<br />Nodes</span>
            <span className="value">{ this.props.deadNodes }</span>
          </div>
        </div>
      </div>
    );
  }
}

function mapStateToNodeLivenessProps(_state: AdminUIState) {
  return {
    liveNodes: 1,
    suspectNodes: 0,
    deadNodes: 0,
  };
}

// tslint:disable-next-line:variable-name
const NodeLivenessConnected = connect(mapStateToNodeLivenessProps)(NodeLiveness);

interface ReplicationStatusProps {
  totalRanges: number;
  underreplicatedRanges: number;
  unavailableRanges: number;
}

class ReplicationStatus extends React.Component<ReplicationStatusProps, {}> {
  render() {
    return (
      <div className="cluster-summary__section">
        <h3 className="cluster-summary__section--title">Replication Status</h3>
        <div className="cluster-summary__section--metrics">
          <div className="cluster-summary__metric">
            <span className="label">Total<br />Ranges</span>
            <span className="value">{ this.props.totalRanges }</span>
          </div>
          <div className="cluster-summary__metric">
            <span className="label">Under-replicated<br />Ranges</span>
            <span className="value">{ this.props.underreplicatedRanges }</span>
          </div>
          <div className="cluster-summary__metric">
            <span className="label">Unavailable<br />Ranges</span>
            <span className="value">{ this.props.unavailableRanges }</span>
          </div>
        </div>
      </div>
    );
  }
}

function mapStateToReplicationStatusProps(_state: AdminUIState) {
  return {
    totalRanges: 2,
    underreplicatedRanges: 0,
    unavailableRanges: 0,
  };
}

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
