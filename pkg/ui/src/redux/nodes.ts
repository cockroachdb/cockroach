// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import _ from "lodash";
import { createSelector } from "reselect";

import * as protos from "src/js/protos";
import { AdminUIState } from "./state";
import { util } from "@cockroachlabs/cluster-ui";
import { Pick } from "src/util/pick";
import { NoConnection } from "src/views/reports/containers/network";
import { nullOfReturnType } from "src/util/types";

/**
 * LivenessStatus is a type alias for the fully-qualified NodeLivenessStatus
 * enumeration. As an enum, it needs to be imported rather than using the 'type'
 * keyword.
 */
export import LivenessStatus = protos.cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus;
import { cockroach } from "src/js/protos";
import MembershipStatus = cockroach.kv.kvserver.liveness.livenesspb.MembershipStatus;
import INodeStatus = cockroach.server.status.statuspb.INodeStatus;
import ILocality = cockroach.roachpb.ILocality;

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
    default:
      return "dead";
  }
}

// Functions to select data directly from the redux state.
const livenessesSelector = (state: AdminUIState) =>
  state.cachedData.liveness.data;

/*
 * nodeStatusesSelector returns the current status for each node in the cluster.
 */
type NodeStatusState = Pick<AdminUIState, "cachedData", "nodes">;
export const nodeStatusesSelector = (state: NodeStatusState) =>
  state.cachedData.nodes.data;

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
  (clusterInfo) => clusterInfo && clusterInfo.cluster_id,
);
/*
 * selectNodeRequestStatus returns the current status of the node status request.
 */
export function selectNodeRequestStatus(state: AdminUIState) {
  return state.cachedData.nodes;
}

/**
 * livenessByNodeIDSelector returns a map from NodeID to the Liveness record for
 * that node.
 */
export const livenessByNodeIDSelector = createSelector(
  livenessesSelector,
  (livenesses) => {
    if (livenesses) {
      return _.keyBy(livenesses.livenesses, (l) => l.node_id);
    }
    return {};
  },
);

/*
 * selectLivenessRequestStatus returns the current status of the liveness request.
 */
export function selectLivenessRequestStatus(state: AdminUIState) {
  return state.cachedData.liveness;
}

/**
 * livenessStatusByNodeIDSelector returns a map from NodeID to the
 * LivenessStatus of that node.
 */
export const livenessStatusByNodeIDSelector = createSelector(
  livenessesSelector,
  (livenesses) => (livenesses ? livenesses.statuses || {} : {}),
);

/*
 * selectCommissionedNodeStatuses returns the node statuses for nodes that have
 * not been decommissioned.
 */
export const selectCommissionedNodeStatuses = createSelector(
  nodeStatusesSelector,
  livenessStatusByNodeIDSelector,
  (nodeStatuses, livenessStatuses) => {
    return _.filter(nodeStatuses, (node) => {
      const livenessStatus = livenessStatuses[`${node.desc.node_id}`];

      return (
        _.isNil(livenessStatus) ||
        livenessStatus !== LivenessStatus.NODE_STATUS_DECOMMISSIONED
      );
    });
  },
);

/**
 * nodeIDsSelector returns the NodeID of all nodes currently on the cluster.
 */
const nodeIDsSelector = createSelector(nodeStatusesSelector, (nodeStatuses) => {
  return _.map(nodeStatuses, (ns) => ns.desc.node_id.toString());
});

/**
 * nodeStatusByIDSelector returns a map from NodeID to a current INodeStatus.
 */
const nodeStatusByIDSelector = createSelector(
  nodeStatusesSelector,
  (nodeStatuses) => {
    const statuses: { [s: string]: INodeStatus } = {};
    _.each(nodeStatuses, (ns) => {
      statuses[ns.desc.node_id.toString()] = ns;
    });
    return statuses;
  },
);

export type NodeSummaryStats = {
  nodeCounts: {
    total: number;
    healthy: number;
    suspect: number;
    dead: number;
    decommissioned: number;
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

/**
 * nodeSumsSelector returns an object with certain cluster-wide totals which are
 * used in different places in the UI.
 */
export const nodeSumsSelector = createSelector(
  nodeStatusesSelector,
  (state: AdminUIState): LivenessResponseStatuses =>
    state.cachedData.liveness.data?.statuses,
  (state: AdminUIState) =>
    state.cachedData.liveness.valid && state.cachedData.nodes.valid,
  sumNodeStats,
);

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
  if (_.isArray(nodeStatuses) && !_.isEmpty(livenessStatusByNodeID)) {
    nodeStatuses.forEach((n) => {
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
) {
  const decommissionedString =
    livenessStatus === LivenessStatus.NODE_STATUS_DECOMMISSIONED
      ? "[decommissioned] "
      : "";

  if (isNoConnection(node)) {
    return `${decommissionedString}(n${node.from.nodeID})`;
  }
  // as the only other type possible right now is INodeStatus we don't have a type guard for that
  return `${decommissionedString}(n${node.desc.node_id}) ${node.desc.address.address_field}`;
}

function isNoConnection(
  node: INodeStatus | NoConnection,
): node is NoConnection {
  return (
    (node as NoConnection).to !== undefined &&
    (node as NoConnection).from !== undefined
  );
}

// nodeDisplayNameByIDSelector provides a unique, human-readable display name
// for each node.
export const nodeDisplayNameByIDSelector = createSelector(
  nodeStatusesSelector,
  livenessStatusByNodeIDSelector,
  (nodeStatuses, livenessStatusByNodeID) => {
    const result: { [key: string]: string } = {};
    if (!_.isEmpty(nodeStatuses)) {
      nodeStatuses.forEach((ns) => {
        result[ns.desc.node_id] = getDisplayName(
          ns,
          livenessStatusByNodeID[ns.desc.node_id],
        );
      });
    }
    return result;
  },
);

export function getRegionFromLocality(locality: ILocality): string {
  for (let i = 0; i < locality.tiers.length; i++) {
    if (locality.tiers[i].key === "region") return locality.tiers[i].value;
  }
  return "";
}

// nodeRegionsByIDSelector provides the region for each node.
export const nodeRegionsByIDSelector = createSelector(
  nodeStatusesSelector,
  (nodeStatuses) => {
    const result: { [key: string]: string } = {};
    if (!_.isEmpty(nodeStatuses)) {
      nodeStatuses.forEach((ns) => {
        result[ns.desc.node_id] = getRegionFromLocality(ns.desc.locality);
      });
    }
    return result;
  },
);

// selectStoreIDsByNodeID returns a map from node ID to a list of store IDs for
// that node. Like nodeIDsSelector, the store ids are converted to strings.
export const selectStoreIDsByNodeID = createSelector(
  nodeStatusesSelector,
  (nodeStatuses) => {
    const result: { [key: string]: string[] } = {};
    _.each(
      nodeStatuses,
      (ns) =>
        (result[ns.desc.node_id] = _.map(ns.store_statuses, (ss) =>
          ss.desc.store_id.toString(),
        )),
    );
    return result;
  },
);

/**
 * nodesSummarySelector returns a directory object containing a variety of
 * computed information based on the current nodes. This object is easy to
 * connect to components on child pages.
 */
export const nodesSummarySelector = createSelector(
  nodeStatusesSelector,
  nodeIDsSelector,
  nodeStatusByIDSelector,
  nodeSumsSelector,
  nodeDisplayNameByIDSelector,
  livenessStatusByNodeIDSelector,
  livenessByNodeIDSelector,
  selectStoreIDsByNodeID,
  (
    nodeStatuses,
    nodeIDs,
    nodeStatusByID,
    nodeSums,
    nodeDisplayNameByID,
    livenessStatusByNodeID,
    livenessByNodeID,
    storeIDsByNodeID,
  ) => {
    return {
      nodeStatuses,
      nodeIDs,
      nodeStatusByID,
      nodeSums,
      nodeDisplayNameByID,
      livenessStatusByNodeID,
      livenessByNodeID,
      storeIDsByNodeID,
    };
  },
);

const nodesSummaryType = nullOfReturnType(nodesSummarySelector);
export type NodesSummary = typeof nodesSummaryType;

// selectNodesSummaryValid is a selector that returns true if the current
// nodesSummary is "valid" (i.e. based on acceptably recent data). This is
// included in the redux-connected state of some pages in order to support
// automatically refreshing data.
export function selectNodesSummaryValid(state: AdminUIState) {
  return state.cachedData.nodes.valid && state.cachedData.liveness.valid;
}

/*
 * clusterNameSelector returns the name of cluster which has to be the same for every node in the cluster.
 * - That is why it is safe to get first non empty cluster name.
 * - Empty cluster name is possible in case `DisableClusterNameVerification` flag is used (see pkg/base/config.go:176).
 */
export const clusterNameSelector = createSelector(
  nodeStatusesSelector,
  livenessStatusByNodeIDSelector,
  (nodeStatuses, livenessStatusByNodeID): string => {
    if (_.isUndefined(nodeStatuses) || _.isEmpty(livenessStatusByNodeID)) {
      return undefined;
    }
    const liveNodesOnCluster = nodeStatuses.filter(
      (nodeStatus) =>
        livenessStatusByNodeID[nodeStatus.desc.node_id] ===
        LivenessStatus.NODE_STATUS_LIVE,
    );

    const nodesWithUniqClusterNames = _.chain(liveNodesOnCluster)
      .filter((node) => !_.isEmpty(node.desc.cluster_name))
      .uniqBy((node) => node.desc.cluster_name)
      .value();

    if (_.isEmpty(nodesWithUniqClusterNames)) {
      return undefined;
    } else {
      return _.head(nodesWithUniqClusterNames).desc.cluster_name;
    }
  },
);

export const versionsSelector = createSelector(
  nodeStatusesSelector,
  livenessByNodeIDSelector,
  (nodeStatuses, livenessStatusByNodeID) =>
    _.chain(nodeStatuses)
      // Ignore nodes for which we don't have any build info.
      .filter((status) => !!status.build_info)
      // Exclude this node if it's known to be decommissioning.
      .filter(
        (status) =>
          !status.desc ||
          !livenessStatusByNodeID[status.desc.node_id] ||
          !livenessStatusByNodeID[status.desc.node_id].membership ||
          !(
            livenessStatusByNodeID[status.desc.node_id].membership !==
            MembershipStatus.ACTIVE
          ),
      )
      // Collect the surviving nodes' build tags.
      .map((status) => status.build_info.tag)
      .uniq()
      .value(),
);

// Select the current build version of the cluster, returning undefined if the
// cluster's version is currently staggered.
export const singleVersionSelector = createSelector(
  versionsSelector,
  (builds) => {
    if (!builds || builds.length !== 1) {
      return undefined;
    }
    return builds[0];
  },
);

/**
 * partitionedStatuses divides the list of node statuses into "live" and "dead".
 */
export const partitionedStatuses = createSelector(
  nodesSummarySelector,
  (summary) => {
    return _.groupBy(summary.nodeStatuses, (ns) => {
      switch (summary.livenessStatusByNodeID[ns.desc.node_id]) {
        case LivenessStatus.NODE_STATUS_LIVE:
        case LivenessStatus.NODE_STATUS_UNAVAILABLE:
        case LivenessStatus.NODE_STATUS_DEAD:
        case LivenessStatus.NODE_STATUS_DECOMMISSIONING:
          return "live";
        case LivenessStatus.NODE_STATUS_DECOMMISSIONED:
          return "decommissioned";
        default:
          // TODO (koorosh): "live" has to be renamed to some partition which
          // represent all except "partitioned" nodes.
          return "live";
      }
    });
  },
);

export const isSingleNodeCluster = createSelector(
  nodeStatusesSelector,
  (nodeStatuses) => nodeStatuses && nodeStatuses.length === 1,
);
