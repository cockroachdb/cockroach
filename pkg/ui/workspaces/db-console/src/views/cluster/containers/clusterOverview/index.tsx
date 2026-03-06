// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { useNodesSummary, util } from "@cockroachlabs/cluster-ui";
import { Skeleton } from "antd";
import classNames from "classnames";
import { format } from "d3-format";
import React, { useMemo } from "react";
import { Helmet } from "react-helmet";

import { sumNodeStats } from "src/redux/nodes";
import EmailSubscription from "src/views/dashboard/emailSubscription";
import OverviewListAlerts from "src/views/shared/containers/alerts/overviewListAlerts";
import createChartComponent from "src/views/shared/util/d3-react";

import capacityChart from "./capacity";
import "./cluster.scss";
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

const formatPercentage = format("0.1%");

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

function ClusterSummary(): React.ReactElement {
  const { nodeStatuses, livenessStatusByNodeID, isLoading } = useNodesSummary();

  const nodeSums = useMemo(
    () => sumNodeStats(nodeStatuses, livenessStatusByNodeID),
    [nodeStatuses, livenessStatusByNodeID],
  );

  const capacityUsage: CapacityUsageProps = {
    usedCapacity: nodeSums.capacityUsed,
    usableCapacity: nodeSums.capacityUsable,
  };

  const nodeLiveness: NodeLivenessProps = {
    liveNodes: nodeSums.nodeCounts.healthy,
    suspectNodes: nodeSums.nodeCounts.suspect,
    deadNodes: nodeSums.nodeCounts.dead,
    drainingNodes: nodeSums.nodeCounts.draining,
  };

  const replicationStatus: ReplicationStatusProps = {
    totalRanges: nodeSums.totalRanges,
    underReplicatedRanges: nodeSums.underReplicatedRanges,
    unavailableRanges: nodeSums.unavailableRanges,
  };

  const children = [
    ...renderCapacityUsage(capacityUsage),
    ...renderNodeLiveness(nodeLiveness),
    ...renderReplicationStatus(replicationStatus),
  ];

  return (
    <section className="cluster-summary">
      <Skeleton
        loading={isLoading}
        active
        // This styling is necessary because this section is a grid.
        style={{ gridColumn: "1 / span all" }}
      >
        {React.Children.toArray(children)}
      </Skeleton>
    </section>
  );
}

/**
 * Renders the main content of the cluster visualization page.
 */
export default function ClusterOverview({
  children,
}: React.PropsWithChildren<{}>): React.ReactElement {
  return (
    <div className="cluster-page">
      <Helmet title="Cluster Overview" />
      <EmailSubscription />
      <OverviewListAlerts />
      <section className="section cluster-overview">
        <ClusterSummary />
      </section>
      <section className="cluster-overview--fixed">{children}</section>
    </div>
  );
}
