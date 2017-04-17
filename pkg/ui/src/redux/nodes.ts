import moment from "moment";
import _ from "lodash";
import { createSelector } from "reselect";

import { AdminUIState } from "./state";
import { NanoToMilli } from "../util/convert";
import { NodeStatus$Properties, MetricConstants, BytesUsed } from "../util/proto";
import { nullOfReturnType } from "../util/types";

/**
 * deadTimeout indicates a grace-period duration where nodes are still
 * considered to be alive; if more than this duration has passed since the
 * node's liveness expired, it is considered dead.
 *
 * The value of this constant is based on the default value of the server
 * configuration value "TimeUntilStoreDead". At some point this configuration
 * value will be moved to a cluster configuration table (#14230), and at that
 * point it will be appropriate to pull this value from the server directly.
 */
export const deadTimeout = moment.duration(5, "m");

/**
 * LivenessStatus is a convenience enumeration used to bucket node liveness
 * records into basic states.
 */
export enum LivenessStatus {
  /**
   * The node's liveness record has been updated recently.
   */
  HEALTHY,
  /**
   * The node's liveness record has not been updated for a short duration, and
   * thus this node might be down.
   */
  SUSPECT,
  /**
   * The node's liveness record has been expired long enough that the node is
   * considered dead.
   */
  DEAD,
}

// Functions to select data directly from the redux state.
export const nodeStatusesSelector = (state: AdminUIState) => state.cachedData.nodes.data;
const livenessesSelector = (state: AdminUIState) => state.cachedData.liveness.data;
const livenessCheckedAtSelector = (state: AdminUIState) => state.cachedData.liveness.setAt;

/**
 * livenessByNodeIDSelector returns a map from NodeID to the Liveness record for
 * that node.
 */
const livenessByNodeIDSelector = createSelector(
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
  livenessByNodeIDSelector,
  livenessCheckedAtSelector,
  (livenessByNodeID, livenessCheckedAt) => {
    if (livenessCheckedAt) {
      const deadCutoff = livenessCheckedAt.clone().subtract(deadTimeout);
      return _.mapValues(livenessByNodeID, (l) => {
        const expiration = moment(NanoToMilli(l.expiration.wall_time.toNumber()));
        if (expiration.isBefore(deadCutoff)) {
          return LivenessStatus.DEAD;
        } else if (expiration.isBefore(livenessCheckedAt)) {
          return LivenessStatus.SUSPECT;
        }
        return LivenessStatus.HEALTHY;
      });
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
      },
      capacityAvailable: 0,
      capacityTotal: 0,
      usedBytes: 0,
      usedMem: 0,
      unavailableRanges: 0,
      replicas: 0,
    };
    if (_.isArray(nodeStatuses) && _.isObject(livenessStatusByNodeID)) {
      nodeStatuses.forEach((n) => {
        result.nodeCounts.total += 1;
        const status = livenessStatusByNodeID[n.desc.node_id];
        switch (status) {
          case LivenessStatus.HEALTHY:
            result.nodeCounts.healthy++;
            break;
          case LivenessStatus.SUSPECT:
            result.nodeCounts.suspect++;
            break;
          case LivenessStatus.DEAD:
            result.nodeCounts.dead++;
            break;
          default:
            result.nodeCounts.dead++;
            break;
        }
        if (status !== LivenessStatus.DEAD) {
          result.capacityAvailable += n.metrics[MetricConstants.availableCapacity];
          result.capacityTotal += n.metrics[MetricConstants.capacity];
          result.usedBytes += BytesUsed(n);
          result.usedMem += n.metrics[MetricConstants.rss];
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
