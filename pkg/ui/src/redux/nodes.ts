import moment from "moment";
import _ from "lodash";
import { createSelector } from "reselect";

import { AdminUIState } from "./state";
import { NanoToMilli } from "../util/convert";
import { NodeStatus, MetricConstants, BytesUsed } from "../util/proto";
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
 * suspectTimeout is similar to deadTimeout, but is a shorter duration and only
 * marks a node as "suspect"; this indicates a node that has an expired
 * liveness, but is not yet being actively repaired.
 *
 * There is no clear server-side analogy to this state, this is simply an
 * indication to the user that something may be wrong with the server.
 */
export const suspectTimeout = moment.duration(1, "m");

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
const nodeStatusesSelector = (state: AdminUIState) => state.cachedData.nodes.data;
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
      return _.keyBy(livenesses.getLivenesses(), (l) => l.getNodeId());
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
      const suspectCutoff = livenessCheckedAt.clone().subtract(suspectTimeout);
      return _.mapValues(livenessByNodeID, (l) => {
        const expiration = moment(NanoToMilli(l.expiration.getWallTime().toNumber()));
        if (expiration.isBefore(deadCutoff)) {
          return LivenessStatus.DEAD;
        } else if (expiration.isBefore(suspectCutoff)) {
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
 * nodeStatusByIDSelector returns a map from NodeID to a current NodeStatus.
 */
const nodeStatusByIDSelector = createSelector(
  nodeStatusesSelector,
  (nodeStatuses) => {
    const statuses: {[s: string]: NodeStatus} = {};
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
        const status = livenessStatusByNodeID[n.getDesc().getNodeId()];
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
          result.capacityAvailable += n.metrics.get(MetricConstants.availableCapacity);
          result.capacityTotal += n.metrics.get(MetricConstants.capacity);
          result.usedBytes += BytesUsed(n);
          result.usedMem += n.metrics.get(MetricConstants.rss);
          result.unavailableRanges += n.metrics.get(MetricConstants.unavailableRanges);
          result.replicas += n.metrics.get(MetricConstants.replicas);
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
