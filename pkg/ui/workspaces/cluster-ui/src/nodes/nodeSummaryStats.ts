// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";

import { BytesUsed, MetricConstants } from "../util/proto";

import { nodeCapacityStats } from "./nodeCapacityStats";

type INodeStatus = cockroach.server.status.statuspb.INodeStatus;

const LivenessStatus =
  cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus;
type LivenessStatus =
  cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus;

export type NodeSummaryStats = {
  nodeCounts: {
    total: number;
    healthy: number;
    suspect: number;
    dead: number;
    decommissioned: number;
    draining: number;
  };
  capacityUsed: number;
  capacityAvailable: number;
  capacityTotal: number;
  capacityUsable: number;
  usedBytes: number;
  usedMem: number;
  totalRanges: number;
  underReplicatedRanges: number;
  unavailableRanges: number;
  replicas: number;
};

/**
 * sumNodeStats aggregates cluster-wide totals from individual node
 * statuses and their liveness information.
 */
export function sumNodeStats(
  nodeStatuses: INodeStatus[],
  livenessStatusByNodeID: Record<string, LivenessStatus>,
): NodeSummaryStats {
  const result: NodeSummaryStats = {
    nodeCounts: {
      total: 0,
      healthy: 0,
      suspect: 0,
      dead: 0,
      decommissioned: 0,
      draining: 0,
    },
    capacityUsed: 0,
    capacityAvailable: 0,
    capacityTotal: 0,
    capacityUsable: 0,
    usedBytes: 0,
    usedMem: 0,
    totalRanges: 0,
    underReplicatedRanges: 0,
    unavailableRanges: 0,
    replicas: 0,
  };

  if (
    !nodeStatuses?.length ||
    !livenessStatusByNodeID ||
    !Object.keys(livenessStatusByNodeID).length
  ) {
    return result;
  }

  for (const n of nodeStatuses) {
    const status = livenessStatusByNodeID[n.desc.node_id.toString()];
    if (status !== LivenessStatus.NODE_STATUS_DECOMMISSIONED) {
      result.nodeCounts.total += 1;
    }
    switch (status) {
      case LivenessStatus.NODE_STATUS_LIVE:
        result.nodeCounts.healthy++;
        break;
      case LivenessStatus.NODE_STATUS_UNAVAILABLE:
      case LivenessStatus.NODE_STATUS_DECOMMISSIONING:
        result.nodeCounts.suspect++;
        break;
      case LivenessStatus.NODE_STATUS_DECOMMISSIONED:
        result.nodeCounts.decommissioned++;
        break;
      case LivenessStatus.NODE_STATUS_DRAINING:
        result.nodeCounts.draining++;
        break;
      case LivenessStatus.NODE_STATUS_DEAD:
      default:
        result.nodeCounts.dead++;
        break;
    }
    if (
      status !== LivenessStatus.NODE_STATUS_DEAD &&
      status !== LivenessStatus.NODE_STATUS_DECOMMISSIONED
    ) {
      const { available, used, usable } = nodeCapacityStats(n);
      result.capacityUsed += used;
      result.capacityAvailable += available;
      result.capacityUsable += usable;
      result.capacityTotal += n.metrics[MetricConstants.capacity];
      result.usedBytes += BytesUsed(n);
      result.usedMem += n.metrics[MetricConstants.rss];
      result.totalRanges += n.metrics[MetricConstants.ranges];
      result.underReplicatedRanges +=
        n.metrics[MetricConstants.underReplicatedRanges];
      result.unavailableRanges += n.metrics[MetricConstants.unavailableRanges];
      result.replicas += n.metrics[MetricConstants.replicas];
    }
  }
  return result;
}
