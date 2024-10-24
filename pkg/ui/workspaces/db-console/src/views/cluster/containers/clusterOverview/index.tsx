// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { util } from "@cockroachlabs/cluster-ui";
import { Skeleton } from "antd";
import classNames from "classnames";
import d3 from "d3";
import React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { createSelector } from "reselect";

import { refreshNodes, refreshLiveness } from "src/redux/apiReducers";
import { nodeSumsSelector } from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import EmailSubscription from "src/views/dashboard/emailSubscription";
import OverviewListAlerts from "src/views/shared/containers/alerts/overviewListAlerts";
import createChartComponent from "src/views/shared/util/d3-react";

import capacityChart from "./capacity";
import "./cluster.styl";
import {
  CapacityUsageTooltip,
  UsedTooltip,
  UsableTooltip,
  LiveNodesTooltip,
  SuspectNodesTooltip,
  DrainingNodesTooltip,
  DeadNodesTooltip,
  TotalRangesTooltip,
  UnderReplicatedRangesTooltip,
  UnavailableRangesTooltip,
} from "./tooltips";

const CapacityChart = createChartComponent("svg", capacityChart());

interface CapacityUsageProps {
  usedCapacity: number;
  usableCapacity: number;
}

const formatPercentage = d3.format("0.1%");

function renderCapacityUsage(props: CapacityUsageProps) {
  const { Bytes } = util;
  const { usedCapacity, usableCapacity } = props;
  const usedPercentage =
    usableCapacity !== 0 ? usedCapacity / usableCapacity : 0;
  return [
    <h3 className="capacity-usage cluster-summary__title">
      <CapacityUsageTooltip>Capacity Usage</CapacityUsageTooltip>
    </h3>,
    <div className="capacity-usage cluster-summary__label storage-percent">
      Used
      <br />
      Percent
    </div>,
    <div className="capacity-usage cluster-summary__metric storage-percent">
      {formatPercentage(usedPercentage)}
    </div>,
    <div className="capacity-usage cluster-summary__chart">
      <CapacityChart used={usedCapacity} usable={usableCapacity} />
    </div>,
    <div className="capacity-usage cluster-summary__label storage-used">
      <UsedTooltip>Used</UsedTooltip>
    </div>,
    <div className="capacity-usage cluster-summary__metric storage-used">
      {Bytes(usedCapacity)}
    </div>,
    <div className="capacity-usage cluster-summary__label storage-usable">
      <UsableTooltip>Usable</UsableTooltip>
    </div>,
    <div className="capacity-usage cluster-summary__metric storage-usable">
      {Bytes(usableCapacity)}
    </div>,
  ];
}

const mapStateToCapacityUsageProps = createSelector(
  nodeSumsSelector,
  nodeSums => {
    const { capacityUsed, capacityUsable } = nodeSums;
    return {
      usedCapacity: capacityUsed,
      usableCapacity: capacityUsable,
    };
  },
);

interface NodeLivenessProps {
  liveNodes: number;
  suspectNodes: number;
  deadNodes: number;
  drainingNodes: number;
}

function renderNodeLiveness(props: NodeLivenessProps) {
  const { liveNodes, suspectNodes, deadNodes, drainingNodes } = props;
  const suspectClasses = classNames(
    "node-liveness",
    "cluster-summary__metric",
    "suspect-nodes",
    {
      warning: suspectNodes > 0,
      disabled: suspectNodes === 0,
    },
  );
  const drainingClasses = classNames(
    "node-liveness",
    "cluster-summary__metric",
    "draining-nodes",
    {
      warning: drainingNodes > 0,
      disabled: drainingNodes === 0,
    },
  );
  const deadClasses = classNames(
    "node-liveness",
    "cluster-summary__metric",
    "dead-nodes",
    {
      alert: deadNodes > 0,
      disabled: deadNodes === 0,
    },
  );
  return [
    <h3 className="node-liveness cluster-summary__title">Node Status</h3>,
    <div className="node-liveness cluster-summary__metric live-nodes">
      {liveNodes}
    </div>,
    <div className="node-liveness cluster-summary__label live-nodes">
      <LiveNodesTooltip>
        Live
        <br />
        Nodes
      </LiveNodesTooltip>
    </div>,
    <div className={suspectClasses}>{suspectNodes}</div>,
    <div className="node-liveness cluster-summary__label suspect-nodes">
      <SuspectNodesTooltip>
        Suspect
        <br />
        Nodes
      </SuspectNodesTooltip>
    </div>,
    <div className={drainingClasses}>{drainingNodes}</div>,
    <div className="node-liveness cluster-summary__label draining-nodes">
      <DrainingNodesTooltip>
        Draining
        <br />
        Nodes
      </DrainingNodesTooltip>
    </div>,
    <div className={deadClasses}>{deadNodes}</div>,
    <div className="node-liveness cluster-summary__label dead-nodes">
      <DeadNodesTooltip>
        Dead
        <br />
        Nodes
      </DeadNodesTooltip>
    </div>,
  ];
}

const mapStateToNodeLivenessProps = createSelector(
  nodeSumsSelector,
  nodeSums => {
    const { nodeCounts } = nodeSums;
    return {
      liveNodes: nodeCounts.healthy,
      suspectNodes: nodeCounts.suspect,
      deadNodes: nodeCounts.dead,
      drainingNodes: nodeCounts.draining,
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
    {
      warning: underReplicatedRanges > 0,
      disabled: underReplicatedRanges === 0,
    },
  );
  const unavailableClasses = classNames(
    "replication-status",
    "cluster-summary__metric",
    "unavailable-ranges",
    {
      alert: unavailableRanges > 0,
      disabled: unavailableRanges === 0,
    },
  );
  return [
    <h3 className="replication-status cluster-summary__title">
      Replication Status
    </h3>,
    <div className="replication-status cluster-summary__metric total-ranges">
      {totalRanges}
    </div>,
    <div className="replication-status cluster-summary__label total-ranges">
      <TotalRangesTooltip>
        Total
        <br />
        Ranges
      </TotalRangesTooltip>
    </div>,
    <div className={underReplicatedClasses}>{underReplicatedRanges}</div>,
    <div className="replication-status cluster-summary__label under-replicated-ranges">
      <UnderReplicatedRangesTooltip>
        Under-replicated
        <br />
        Ranges
      </UnderReplicatedRangesTooltip>
    </div>,
    <div className={unavailableClasses}>{unavailableRanges}</div>,
    <div className="replication-status cluster-summary__label unavailable-ranges">
      <UnavailableRangesTooltip>
        Unavailable
        <br />
        Ranges
      </UnavailableRangesTooltip>
    </div>,
  ];
}

const mapStateToReplicationStatusProps = createSelector(
  nodeSumsSelector,
  nodeSums => {
    const { totalRanges, underReplicatedRanges, unavailableRanges } = nodeSums;
    return {
      totalRanges: totalRanges,
      underReplicatedRanges: underReplicatedRanges,
      unavailableRanges: unavailableRanges,
    };
  },
);

interface ClusterSummaryStateProps {
  capacityUsage: CapacityUsageProps;
  nodeLiveness: NodeLivenessProps;
  replicationStatus: ReplicationStatusProps;
  loading: boolean;
}
interface ClusterSummaryActionsProps {
  refreshLiveness: () => void;
  refreshNodes: () => void;
}

type ClusterSummaryProps = ClusterSummaryStateProps &
  ClusterSummaryActionsProps;

class ClusterSummary extends React.Component<ClusterSummaryProps, {}> {
  componentDidMount() {
    this.refresh();
  }

  componentDidUpdate() {
    this.refresh();
  }

  refresh() {
    this.props.refreshLiveness();
    this.props.refreshNodes();
  }

  render() {
    const children = [
      ...renderCapacityUsage(this.props.capacityUsage),
      ...renderNodeLiveness(this.props.nodeLiveness),
      ...renderReplicationStatus(this.props.replicationStatus),
    ];

    return (
      <section className="cluster-summary">
        <Skeleton
          loading={this.props.loading}
          active
          // This styling is necessary because this section is a grid.
          style={{ gridColumn: "1 / span all" }}
        >
          {React.Children.toArray(children)}
        </Skeleton>
      </section>
    );
  }
}

function mapStateToClusterSummaryProps(state: AdminUIState) {
  return {
    capacityUsage: mapStateToCapacityUsageProps(state),
    nodeLiveness: mapStateToNodeLivenessProps(state),
    replicationStatus: mapStateToReplicationStatusProps(state),
    loading: !state.cachedData.nodes.data,
  };
}

const actions = {
  refreshLiveness: refreshLiveness,
  refreshNodes: refreshNodes,
};

const ClusterSummaryConnected = connect(
  mapStateToClusterSummaryProps,
  actions,
)(ClusterSummary);

/**
 * Renders the main content of the cluster visualization page.
 */
export default class ClusterOverview extends React.Component<any, any> {
  render() {
    return (
      <div className="cluster-page">
        <Helmet title="Cluster Overview" />
        <EmailSubscription />
        <OverviewListAlerts />
        <section className="section cluster-overview">
          <ClusterSummaryConnected />
        </section>
        <section className="cluster-overview--fixed">
          {this.props.children}
        </section>
      </div>
    );
  }
}
