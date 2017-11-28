import classNames from "classnames";
import d3 from "d3";
import React from "react";
import { connect } from "react-redux";
import { createSelector } from "reselect";

import { AdminUIState } from "src/redux/state";
import { nodesSummarySelector, NodesSummary } from "src/redux/nodes";
import { Bytes as formatBytes } from "src/util/format";
import { NodesOverview } from "src/views/cluster/containers/nodesOverview";
import createChartComponent from "src/views/shared/util/d3-react";
import capacityChart from "./capacity";

import "./cluster.styl";

// tslint:disable-next-line:variable-name
const CapacityChart = createChartComponent(capacityChart);

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

function renderCapacityUsage(props: CapacityUsageProps) {
  const { usedCapacity, usableCapacity } = props;
  const usedPercentage = usableCapacity !== 0 ? usedCapacity / usableCapacity : 0;
  return [
    <h3 className="capacity-usage cluster-summary__title">Capacity Usage</h3>,
    <div className="capacity-usage cluster-summary__metric">{ formatPercentage(usedPercentage) }</div>,
    <div className="capacity-usage cluster-summary__chart">
      <CapacityChart used={usedCapacity} usable={usableCapacity} />
    </div>,
    <div className="capacity-usage cluster-summary__aside">
      <span className="label">Current Usage</span>
      <span className="value">{ formatBytes(usedCapacity) }</span>
    </div>,
  ];
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

interface NodeLivenessProps {
  liveNodes: number;
  suspectNodes: number;
  deadNodes: number;
}

function renderNodeLiveness(props: NodeLivenessProps) {
  const { liveNodes, suspectNodes, deadNodes } = props;
  const suspectClasses = classNames(
    "node-liveness",
    "cluster-summary__metric",
    "suspect-nodes",
    {"warning": suspectNodes > 0 },
  );
  const deadClasses = classNames(
    "node-liveness",
    "cluster-summary__metric",
    "dead-nodes",
    { "alert": deadNodes > 0 },
  );
  return [
    <h3 className="node-liveness cluster-summary__title">Node Liveness</h3>,
    <div className="node-liveness cluster-summary__metric live-nodes">{ liveNodes }</div>,
    <div className="node-liveness cluster-summary__label live-nodes">Live<br />Nodes</div>,
    <div className={suspectClasses}>{ suspectNodes }</div>,
    <div className="node-liveness cluster-summary__label suspect-nodes">Suspect<br />Nodes</div>,
    <div className={deadClasses}>{ deadNodes }</div>,
    <div className="node-liveness cluster-summary__label dead-nodes">Dead<br />Nodes</div>,
  ];
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

interface ReplicationStatusProps {
  totalRanges: number;
  underReplicatedRanges: number;
  unavailableRanges: number;
}

function renderReplicationStatus(props: ReplicationStatusProps) {
  const { totalRanges, underReplicatedRanges, unavailableRanges } = props;
  const underReplicatedClasses = classNames(
    "replication-status",
    "cluster-summary__metric",
    "under-replicated-ranges",
    { "warning": underReplicatedRanges > 0 },
  );
  const unavailableClasses = classNames(
    "replication-status",
    "cluster-summary__metric",
    "unavailable-ranges",
    { "alert": unavailableRanges > 0 },
  );
  return [
    <h3 className="replication-status cluster-summary__title">Replication Status</h3>,
    <div className="replication-status cluster-summary__metric total-ranges">{ totalRanges }</div>,
    <div className="replication-status cluster-summary__label total-ranges">Total<br />Ranges</div>,
    <div className={underReplicatedClasses}>{ underReplicatedRanges }</div>,
    <div className="replication-status cluster-summary__label under-replicated-ranges">Under-replicated<br />Ranges</div>,
    <div className={unavailableClasses}>{ unavailableRanges }</div>,
    <div className="replication-status cluster-summary__label unavailable-ranges">Unavailable<br />Ranges</div>,
  ];
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

interface ClusterSummaryProps {
  capacityUsage: CapacityUsageProps;
  nodeLiveness: NodeLivenessProps;
  replicationStatus: ReplicationStatusProps;
}

class ClusterSummary extends React.Component<ClusterSummaryProps, {}> {
  render() {
    const children = [
      ...renderCapacityUsage(this.props.capacityUsage),
      ...renderNodeLiveness(this.props.nodeLiveness),
      ...renderReplicationStatus(this.props.replicationStatus),
    ];
    return <section className="cluster-summary" children={children} />;
  }
}

function mapStateToClusterSummaryProps (state: AdminUIState) {
  return {
    capacityUsage: mapStateToCapacityUsageProps(state),
    nodeLiveness: mapStateToNodeLivenessProps(state),
    replicationStatus: mapStateToReplicationStatusProps(state),
  };
}

// tslint:disable-next-line:variable-name
const ClusterSummaryConnected = connect(mapStateToClusterSummaryProps)(ClusterSummary);

/**
 * Renders the main content of the cluster visualization page.
 */
class ClusterOverview extends React.Component<{}, {}> {
  render() {
    return (
      <div>
        <div className="cluster-overview">
          <ClusterTicker />
          <ClusterSummaryConnected />
        </div>
        <NodesOverview />
      </div>
    );
  }
}

export { ClusterOverview as default };
