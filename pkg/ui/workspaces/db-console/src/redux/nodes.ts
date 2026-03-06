// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { util } from "@cockroachlabs/cluster-ui";
import countBy from "lodash/countBy";
import filter from "lodash/filter";
import first from "lodash/first";
import flow from "lodash/flow";
import head from "lodash/head";
import isArray from "lodash/isArray";
import isEmpty from "lodash/isEmpty";
import isUndefined from "lodash/isUndefined";
import map from "lodash/map";
import sortBy from "lodash/sortBy";
import uniq from "lodash/uniq";
import uniqBy from "lodash/uniqBy";
import { createSelector } from "reselect";

import * as protos from "src/js/protos";
import { cockroach } from "src/js/protos";
import { NoConnection } from "src/views/reports/containers/network";

import { AdminUIState } from "./state";

/**
 * LivenessStatus is a type alias for the fully-qualified NodeLivenessStatus
 * enumeration. As an enum, it needs to be imported rather than using the 'type'
 * keyword.
 */
export import LivenessStatus = protos.cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus;

import MembershipStatus = cockroach.kv.kvserver.liveness.livenesspb.MembershipStatus;
import INodeStatus = cockroach.server.status.statuspb.INodeStatus;

const { MetricConstants, BytesUsed } = util;

/**
 * livenessNomenclature resolves a mismatch between the terms used for liveness
 * status on our Admin UI and the terms used by the backend. Examples:
 * + "Live" on the server is "Healthy" on the Admin UI
 * + "Unavailable" on the server is "Suspect" on the Admin UI
 */
export function livenessNomenclature(liveness: LivenessStatus) {
  switch (liveness) {
    case LivenessStatus.NODE_STATUS_LIVE:
      return "healthy";
    case LivenessStatus.NODE_STATUS_UNAVAILABLE:
      return "suspect";
    case LivenessStatus.NODE_STATUS_DECOMMISSIONING:
      return "decommissioning";
    case LivenessStatus.NODE_STATUS_DECOMMISSIONED:
      return "decommissioned";
    case LivenessStatus.NODE_STATUS_DRAINING:
      return "draining";
    default:
      return "dead";
  }
}

/*
 * clusterSelector returns information about cluster.
 */
export const clusterSelector = (state: AdminUIState) =>
  state.cachedData.cluster.data;

/*
 * clusterIdSelector returns Cluster Id (as UUID string).
 */
export const clusterIdSelector = createSelector(
  clusterSelector,
  clusterInfo => clusterInfo && clusterInfo.cluster_id,
);

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

export type LivenessResponseStatuses = { [id: string]: LivenessStatus };

export function sumNodeStats(
  nodeStatuses: INodeStatus[],
  livenessStatusByNodeID: LivenessResponseStatuses,
): NodeSummaryStats {
  const result = {
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
  if (isArray(nodeStatuses) && !isEmpty(livenessStatusByNodeID)) {
    nodeStatuses.forEach(n => {
      const status = livenessStatusByNodeID[n.desc.node_id];
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
        result.unavailableRanges +=
          n.metrics[MetricConstants.unavailableRanges];
        result.replicas += n.metrics[MetricConstants.replicas];
      }
    });
  }
  return result;
}

export interface CapacityStats {
  used: number;
  usable: number;
  available: number;
}

export function nodeCapacityStats(n: INodeStatus): CapacityStats {
  const used = n.metrics[MetricConstants.usedCapacity];
  const available = n.metrics[MetricConstants.availableCapacity];
  return {
    used,
    available,
    usable: used + available,
  };
}

export function getDisplayName(
  node: INodeStatus | NoConnection,
  livenessStatus = LivenessStatus.NODE_STATUS_LIVE,
  includeAddress = true,
) {
  const decommissionedString =
    livenessStatus === LivenessStatus.NODE_STATUS_DECOMMISSIONED
      ? "[decommissioned] "
      : "";

  if (isNoConnection(node)) {
    return `${decommissionedString}(n${node.from.nodeID})`;
  }
  // as the only other type possible right now is INodeStatus we don't have a type guard for that
  if (includeAddress) {
    return `${decommissionedString}(n${node.desc.node_id}) ${node.desc.address.address_field}`;
  } else {
    return `${decommissionedString}n${node.desc.node_id}`;
  }
}

function isNoConnection(
  node: INodeStatus | NoConnection,
): node is NoConnection {
  return (
    (node as NoConnection).to !== undefined &&
    (node as NoConnection).from !== undefined
  );
}

/**
 * getClusterName returns the cluster name from live nodes. All nodes in a
 * cluster share the same cluster name; this returns the first non-empty one.
 */
export function getClusterName(
  nodeStatuses: INodeStatus[],
  livenessStatusByNodeID: { [id: string]: LivenessStatus },
): string {
  if (isUndefined(nodeStatuses) || isEmpty(livenessStatusByNodeID)) {
    return undefined;
  }
  const liveNodesOnCluster = nodeStatuses.filter(
    nodeStatus =>
      livenessStatusByNodeID[nodeStatus.desc.node_id] ===
      LivenessStatus.NODE_STATUS_LIVE,
  );

  const nodesWithUniqClusterNames = flow(
    (statuses: INodeStatus[]) =>
      filter(statuses, s => !isEmpty(s.desc.cluster_name)),
    statuses => uniqBy(statuses, s => s.desc.cluster_name),
  )(liveNodesOnCluster);

  if (isEmpty(nodesWithUniqClusterNames)) {
    return undefined;
  } else {
    return head(nodesWithUniqClusterNames).desc.cluster_name;
  }
}

/**
 * validateNodes filters node statuses to only include nodes with build_info
 * and active membership status. Used for version-related computations.
 */
export function validateNodes(
  nodeStatuses: INodeStatus[],
  livenessByNodeID: {
    [id: string]: cockroach.kv.kvserver.liveness.livenesspb.ILiveness;
  },
): INodeStatus[] {
  if (!nodeStatuses) {
    return undefined;
  }
  return (
    nodeStatuses
      // Ignore nodes for which we don't have any build info.
      .filter(status => !!status.build_info)
      // Exclude this node if it's known to be decommissioning.
      .filter(
        status =>
          !status.desc ||
          !livenessByNodeID[status.desc.node_id] ||
          !livenessByNodeID[status.desc.node_id].membership ||
          livenessByNodeID[status.desc.node_id].membership ===
            MembershipStatus.ACTIVE,
      )
  );
}

/**
 * getVersions returns unique build tags from validated nodes.
 */
export function getVersions(nodes: INodeStatus[]): string[] {
  return uniq(map(nodes, status => status.build_info.tag));
}

/**
 * getNumNodesByVersionsTag returns a count of nodes by build tag.
 */
export function getNumNodesByVersionsTag(
  nodes: INodeStatus[],
): Map<string, number> {
  if (!nodes) {
    return new Map();
  }
  return new Map(Object.entries(countBy(nodes, node => node?.build_info?.tag)));
}

/**
 * getNumNodesByVersions returns a count of nodes by major.minor version.
 */
export function getNumNodesByVersions(
  nodes: INodeStatus[],
): Map<string, number> {
  if (!nodes) {
    return new Map();
  }
  return new Map(
    Object.entries(
      countBy(nodes, node => {
        const serverVersion = node?.desc?.ServerVersion;
        if (serverVersion) {
          return `${serverVersion.major_val}.${serverVersion.minor_val}`;
        }
        return "";
      }),
    ),
  );
}

/**
 * getSingleVersion returns the single build version of the cluster, or
 * undefined if the cluster's version is staggered.
 */
export function getSingleVersion(builds: string[]): string {
  if (!builds || builds.length !== 1) {
    return undefined;
  }
  return builds[0];
}

/**
 * getClusterVersionLabel returns the formatted version label for the cluster.
 * If multiple versions are running, it shows the lowest version with a
 * "Mixed Versions" suffix.
 */
export function getClusterVersionLabel(builds: string[]): string {
  if (!builds) {
    return undefined;
  }
  if (builds.length > 1) {
    const lowestVersion = first(sortBy(builds, b => b));
    return `${lowestVersion} - Mixed Versions`;
  }
  return builds[0];
}
