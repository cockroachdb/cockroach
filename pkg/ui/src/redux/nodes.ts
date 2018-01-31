import moment from "moment";
import _ from "lodash";
import { createSelector } from "reselect";

import * as protos from "src/js/protos";
import { AdminUIState } from "./state";
import { Pick } from "src/util/pick";
import { NodeStatus$Properties, MetricConstants, BytesUsed } from "src/util/proto";
import { nullOfReturnType } from "src/util/types";

/**
 * LivenessStatus is a type alias for the fully-qualified NodeLivenessStatus
 * enumeration. As an enum, it needs to be imported rather than using the 'type'
 * keyword.
 */
export import LivenessStatus = protos.cockroach.storage.NodeLivenessStatus;

/**
 * livenessNomenclature resolves a mismatch between the terms used for liveness
 * status on our Admin UI and the terms used by the backend. Examples:
 * + "Live" on the server is "Healthy" on the Admin UI
 * + "Unavailable" on the server is "Suspect" on the Admin UI
 */
export function livenessNomenclature(liveness: LivenessStatus) {
  switch (liveness) {
    case LivenessStatus.LIVE:
      return "healthy";
    case LivenessStatus.UNAVAILABLE:
      return "suspect";
    case LivenessStatus.DECOMMISSIONING:
      return "decommissioned";
    default:
      return "dead";
  }
}

// Functions to select data directly from the redux state.
const livenessesSelector = (state: AdminUIState) => state.cachedData.liveness.data;

/*
 * nodeStatusesSelector returns the current status for each node in the cluster.
 */
type NodeStatusState = Pick<AdminUIState, "cachedData", "nodes">;
export const nodeStatusesSelector = (state: NodeStatusState) => state.cachedData.nodes.data;
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

/**
 * livenessStatusByNodeIDSelector returns a map from NodeID to the
 * LivenessStatus of that node.
 */
const livenessStatusByNodeIDSelector = createSelector(
  livenessesSelector,
  (livenesses) => {
    if (livenesses) {
      return livenesses.statuses;
    }
    return {};
  },
);

/**
 * nodeIDsSelector returns the NodeID of all nodes currently on the cluster.
 */
const nodeIDsSelector = createSelector(
  nodeStatusesSelector,
  (nodeStatuses) => {
    return _.map(nodeStatuses, (ns) => ns.desc.node_id.toString());
  },
);

/**
 * nodeStatusByIDSelector returns a map from NodeID to a current NodeStatus$Properties.
 */
const nodeStatusByIDSelector = createSelector(
  nodeStatusesSelector,
  (nodeStatuses) => {
    const statuses: {[s: string]: NodeStatus$Properties} = {};
    _.each(nodeStatuses, (ns) => {
      statuses[ns.desc.node_id.toString()] = ns;
    });
    return statuses;
  },
);

/**
 * nodeSumsSelector returns an object with certain cluster-wide totals which are
 * used in different places in the UI.
 */
const nodeSumsSelector = createSelector(
  nodeStatusesSelector,
  livenessStatusByNodeIDSelector,
  (nodeStatuses, livenessStatusByNodeID) => {
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
      usedBytes: 0,
      usedMem: 0,
      totalRanges: 0,
      underReplicatedRanges: 0,
      unavailableRanges: 0,
      replicas: 0,
    };
    if (_.isArray(nodeStatuses) && _.isObject(livenessStatusByNodeID)) {
      nodeStatuses.forEach((n) => {
        const status = livenessStatusByNodeID[n.desc.node_id];
        if (status !== LivenessStatus.DECOMMISSIONING) {
          result.nodeCounts.total += 1;
        }
        switch (status) {
          case LivenessStatus.LIVE:
            result.nodeCounts.healthy++;
            break;
          case LivenessStatus.UNAVAILABLE:
            result.nodeCounts.suspect++;
            break;
          case LivenessStatus.DECOMMISSIONING:
            result.nodeCounts.decommissioned++;
            break;
          case LivenessStatus.DEAD:
          default:
            result.nodeCounts.dead++;
            break;
        }
        if (status !== LivenessStatus.DEAD) {
          result.capacityUsed += n.metrics[MetricConstants.usedCapacity];
          result.capacityAvailable += n.metrics[MetricConstants.availableCapacity];
          result.capacityTotal += n.metrics[MetricConstants.capacity];
          result.usedBytes += BytesUsed(n);
          result.usedMem += n.metrics[MetricConstants.rss];
          result.totalRanges += n.metrics[MetricConstants.ranges];
          result.underReplicatedRanges += n.metrics[MetricConstants.underReplicatedRanges];
          result.unavailableRanges += n.metrics[MetricConstants.unavailableRanges];
          result.replicas += n.metrics[MetricConstants.replicas];
        }
      });
    }
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
  livenessStatusByNodeIDSelector,
  livenessByNodeIDSelector,
  (nodeStatuses, nodeIDs, nodeStatusByID, nodeSums, livenessStatusByNodeID, livenessByNodeID) => {
    return {
      nodeStatuses,
      nodeIDs,
      nodeStatusByID,
      nodeSums,
      livenessStatusByNodeID,
      livenessByNodeID,
    };
  },
);

const nodesSummaryType = nullOfReturnType(nodesSummarySelector);
export type NodesSummary = typeof nodesSummaryType;
