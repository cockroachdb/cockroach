// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { util } from "@cockroachlabs/cluster-ui";
import countBy from "lodash/countBy";
import each from "lodash/each";
import filter from "lodash/filter";
import first from "lodash/first";
import flow from "lodash/flow";
import groupBy from "lodash/groupBy";
import head from "lodash/head";
import isArray from "lodash/isArray";
import isEmpty from "lodash/isEmpty";
import isNil from "lodash/isNil";
import isUndefined from "lodash/isUndefined";
import keyBy from "lodash/keyBy";
import map from "lodash/map";
import sortBy from "lodash/sortBy";
import uniq from "lodash/uniq";
import uniqBy from "lodash/uniqBy";
import { createSelector } from "reselect";

import * as protos from "src/js/protos";
import { cockroach } from "src/js/protos";
import { Pick } from "src/util/pick";
import { nullOfReturnType } from "src/util/types";
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
    case LivenessStatus.NODE_STATUS_DRAINING:
      return "draining";
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

// partialNodeStatusesSelector returns NodeStatus items without fields that constantly change
// and causes selectors to recompute.
const partialNodeStatusesSelector = createSelector(
  nodeStatusesSelector,
  (nodeStatuses: INodeStatus[]) => {
    return nodeStatuses?.map((ns: INodeStatus) => {
      // We need to extract the fields that constantly change below, so
      // suppress the eslint rule.
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      const { metrics, updated_at, activity, store_statuses, ...rest } = ns;
      return {
        ...rest,
        store_statuses: store_statuses?.map(ss => ({ desc: ss.desc })),
      };
    });
  },
);

export const selectNodesLastError = createSelector(
  (state: AdminUIState) => state.cachedData.nodes,
  nodes => nodes.lastError,
);

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
  livenesses => {
    if (livenesses) {
      return keyBy(livenesses.livenesses, l => l.node_id);
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
  livenesses => (livenesses ? livenesses.statuses || {} : {}),
);

/*
 * selectCommissionedNodeStatuses returns the node statuses for nodes that have
 * not been decommissioned.
 */
export const selectCommissionedNodeStatuses = createSelector(
  nodeStatusesSelector,
  livenessStatusByNodeIDSelector,
  (nodeStatuses, livenessStatuses) => {
    return filter(nodeStatuses, node => {
      const livenessStatus = livenessStatuses[`${node.desc.node_id}`];

      return (
        isNil(livenessStatus) ||
        livenessStatus !== LivenessStatus.NODE_STATUS_DECOMMISSIONED
      );
    });
  },
);

/**
 * nodeIDsSelector returns the NodeID of all nodes currently on the cluster.
 */
export const nodeIDsSelector = createSelector(
  partialNodeStatusesSelector,
  nodeStatuses => map(nodeStatuses, ns => ns.desc.node_id),
);

/**
 * nodeIDsStringifiedSelector returns available node IDs on cluster as list of strings.
 */
export const nodeIDsStringifiedSelector = createSelector(nodeIDsSelector, ids =>
  ids.map(id => id.toString()),
);

/**
 * nodeStatusByIDSelector returns a map from NodeID to a current INodeStatus.
 */
export const nodeStatusByIDSelector = createSelector(
  nodeStatusesSelector,
  nodeStatuses => {
    const statuses: { [s: string]: INodeStatus } = {};
    each(nodeStatuses, ns => {
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

// nodeDisplayNameByIDSelector provides a unique, human-readable display name
// for each node.

// This function will never be passed decommissioned nodes because
// #56529 removed a node's status entry once it's decommissioned.
export const nodeDisplayNameByIDSelector = createSelector(
  partialNodeStatusesSelector,
  livenessStatusByNodeIDSelector,
  (nodeStatuses, livenessStatusByNodeID) => {
    const result: { [key: string]: string } = {};
    if (!isEmpty(nodeStatuses)) {
      nodeStatuses.forEach(ns => {
        result[ns.desc.node_id] = getDisplayName(
          ns,
          livenessStatusByNodeID[ns.desc.node_id],
          true,
        );
      });
    }
    return result;
  },
);

export const nodeDisplayNameByIDSelectorWithoutAddress = createSelector(
  partialNodeStatusesSelector,
  livenessStatusByNodeIDSelector,
  (nodeStatuses, livenessStatusByNodeID) => {
    const result: { [key: string]: string } = {};
    if (!isEmpty(nodeStatuses)) {
      nodeStatuses.forEach(ns => {
        result[ns.desc.node_id] = getDisplayName(
          ns,
          livenessStatusByNodeID[ns.desc.node_id],
          false,
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
  nodeStatuses => {
    const result: { [key: string]: string } = {};
    if (!isEmpty(nodeStatuses)) {
      nodeStatuses.forEach(ns => {
        result[ns.desc.node_id] = getRegionFromLocality(ns.desc.locality);
      });
    }
    return result;
  },
);

// selectIsMoreThanOneNode returns a boolean describing whether or not there
// exists more than one node in the cluster.
export const selectIsMoreThanOneNode = createSelector(
  (state: AdminUIState) => nodeRegionsByIDSelector(state),
  (nodeRegions): boolean => {
    return Object.keys(nodeRegions).length > 1;
  },
);

// selectStoreIDsByNodeID returns a map from node ID to a list of store IDs for
// that node. Like nodeIDsSelector, the store ids are converted to strings.
export const selectStoreIDsByNodeID = createSelector(
  partialNodeStatusesSelector,
  nodeStatuses => {
    const result: { [key: string]: string[] } = {};
    each(
      nodeStatuses,
      ns =>
        (result[ns.desc.node_id] = map(ns.store_statuses, ss =>
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
  nodeIDsStringifiedSelector,
  nodeStatusByIDSelector,
  nodeDisplayNameByIDSelector,
  livenessStatusByNodeIDSelector,
  livenessByNodeIDSelector,
  selectStoreIDsByNodeID,
  selectNodesLastError,
  (
    nodeStatuses,
    nodeIDs,
    nodeStatusByID,
    nodeDisplayNameByID,
    livenessStatusByNodeID,
    livenessByNodeID,
    storeIDsByNodeID,
    nodeLastError,
  ) => {
    return {
      nodeStatuses,
      nodeIDs,
      nodeStatusByID,
      nodeDisplayNameByID,
      livenessStatusByNodeID,
      livenessByNodeID,
      storeIDsByNodeID,
      nodeLastError,
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
  },
);

export const validateNodesSelector = createSelector(
  nodeStatusesSelector,
  livenessByNodeIDSelector,
  (nodeStatuses, livenessStatusByNodeID) => {
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
            !livenessStatusByNodeID[status.desc.node_id] ||
            !livenessStatusByNodeID[status.desc.node_id].membership ||
            !(
              livenessStatusByNodeID[status.desc.node_id].membership !==
              MembershipStatus.ACTIVE
            ),
        )
    );
  },
);

export const versionsSelector = createSelector(validateNodesSelector, nodes => {
  return uniq(map(nodes, status => status.build_info.tag));
});

export const numNodesByVersionsTagSelector = createSelector(
  validateNodesSelector,
  nodes => {
    if (!nodes) {
      return new Map();
    }
    return new Map(
      Object.entries(countBy(nodes, node => node?.build_info?.tag)),
    );
  },
);

export const numNodesByVersionsSelector = createSelector(
  validateNodesSelector,
  nodes => {
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
  },
);

// Select the current build version of the cluster, returning undefined if the
// cluster's version is currently staggered.
export const singleVersionSelector = createSelector(
  versionsSelector,
  builds => {
    if (!builds || builds.length !== 1) {
      return undefined;
    }
    return builds[0];
  },
);

// clusterVersionSelector returns build version of the cluster, or returns the lowest version
// if cluster's version is staggered.
export const clusterVersionLabelSelector = createSelector(
  versionsSelector,
  builds => {
    if (!builds) {
      return undefined;
    }
    if (builds.length > 1) {
      const lowestVersion = first(sortBy(builds, b => b));
      return `${lowestVersion} - Mixed Versions`;
    }
    return builds[0];
  },
);

/**
 * partitionedStatuses divides the list of node statuses into "live" and "dead".
 */
export const partitionedStatuses = createSelector(
  nodesSummarySelector,
  summary => {
    return groupBy(summary.nodeStatuses, ns => {
      switch (summary.livenessStatusByNodeID[ns.desc.node_id]) {
        case LivenessStatus.NODE_STATUS_LIVE:
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
  nodeStatuses => nodeStatuses && nodeStatuses.length === 1,
);
