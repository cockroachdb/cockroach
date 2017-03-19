import moment from "moment";
import _ from "lodash";
import { createSelector } from "reselect";

import { AdminUIState } from "./state";
import { NanoToMilli } from "../util/convert";
import { NodeStatus, MetricConstants, BytesUsed } from "../util/proto";

// deadTimeout indicates a grace-period duration where nodes are still
// considered to be alive; if more than this duration has passed since the
// node's liveness expired, it is considered dead.
export const deadTimeout = moment.duration(5, "m");

// suspectTimeout is similar to deadTimeout, but is a shorter duration and only
// marks a node as "suspect"; this indicates a node that has an expired
// liveness, but is not yet being actively repaired.
export const suspectTimeout = moment.duration(1, "m");

// LivenessStatus is a convenience enumeration used to bucket node liveness
// records into basic states.
export enum LivenessStatus {
  // The node's liveness record has been updated recently.
  HEALTHY,
  // The node's liveness record has not been updated for a short duration, and
  // thus this node might be down.
  SUSPECT,
  // The node's liveness record has been expired long enough that the node is
  // considered dead.
  DEAD,
}

// Functions to select data directly from the redux state.
let nodeStatuses = (state: AdminUIState) => state.cachedData.nodes.data;
let livenesses = (state: AdminUIState) => state.cachedData.liveness.data;
let livenessCheckedAt = (state: AdminUIState) => state.cachedData.liveness.setAt;

// livenessByNodeID returns a map from NodeID to the Liveness record for that
// node.
let livenessByNodeID = createSelector(
  livenesses,
  (live) => {
    if (live) {
      return _.keyBy(live.getLivenesses(), (l) => l.getNodeId());
    }
    return {};
  },
);

// livenessStatusByNodeID returns a map from NodeID to the LivenessStatus of
// that node.
let livenessStatusByNodeID = createSelector(
  livenessByNodeID,
  livenessCheckedAt,
  (liveByNodeID, liveCheckedAt) => {
    if (liveCheckedAt) {
      let deadCutoff = liveCheckedAt.clone().subtract(deadTimeout);
      let suspectCutoff = liveCheckedAt.clone().subtract(suspectTimeout);
      return _.mapValues(liveByNodeID, (l) => {
        let expiration = moment(NanoToMilli(l.expiration.getWallTime().toNumber()));
        let status = LivenessStatus.HEALTHY;
        if (expiration.isBefore(deadCutoff)) {
          status = LivenessStatus.DEAD;
        } else if (expiration.isBefore(suspectCutoff)) {
          status = LivenessStatus.SUSPECT;
        }
        return status;
      });
    }
    return {};
  },
);

// nodeIDs returns the NodeID of all nodes currently on the cluster.
let nodeIDs = createSelector(
  nodeStatuses,
  (nss) => {
    return _.map(nss, (ns) => {
      return ns.desc.node_id.toString();
    });
  },
);

// nodeStatusByID returns a map from NodeID to a current NodeStatus.
let nodeStatusByID = createSelector(
  nodeStatuses,
  (nss) => {
    let statuses: {[s: string]: NodeStatus} = {};
    _.each(nss, (ns) => {
      statuses[ns.desc.node_id.toString()] = ns;
    });
    return statuses;
  },
);

// nodeSums returns an object with certain cluster-wide totals which are used
// in different places in the UI.
let nodeSums = createSelector(
  nodeStatuses,
  livenessStatusByNodeID,
  (ns, liveMap) => {
    let result = {
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
    if (_.isArray(ns) && _.isObject(liveMap)) {
      ns.forEach((n) => {
        result.nodeCounts.total += 1;
        let status = liveMap[n.getDesc().getNodeId()];
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

// nodesSummary returns a directory object containing a variety of computed
// information based on the current nodes. This object is easy to connect
// to components on child pages.
export let nodesSummary = createSelector(
  nodeStatuses,
  nodeIDs,
  nodeStatusByID,
  nodeSums,
  livenessStatusByNodeID,
  livenessByNodeID,
  // tslint:disable-next-line:no-shadowed-variable
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

// getReturnType allows us to extract the return type of the nodesSummary
// selector; this is essentially a "returntypeof", which typescript does not
// have. We create a function that returns the same return type as nodesSummary,
// but return a null value. We then create a variable and assign it the output
// of this function. Finally, we can use 'typeof' to get the type of that
// variable, which is the same as the type of nodesSummary's return.
function getReturnType<R> (_: (...args: any[]) => R): R {
    return null;
}
let nodesSummaryType = getReturnType(nodesSummary);
export type NodesSummary = typeof nodesSummaryType;
