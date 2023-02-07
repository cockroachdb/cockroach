// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import _ from "lodash";
import { cockroach } from "src/js/protos";
import { createSelector } from "reselect";
import { util } from "@cockroachlabs/cluster-ui";
import { AdminUIState } from "src/redux/state";
import {
  LivenessStatus,
  nodesSummarySelector,
  selectLivenessRequestStatus,
  selectNodeRequestStatus,
} from "src/redux/nodes";
import { getValueFromString, Identity, NoConnection } from ".";
import { localityToString } from "../../components/nodeFilterList";

import MembershipStatus = cockroach.kv.kvserver.liveness.livenesspb.MembershipStatus;
import { FixLong } from "src/util/fixLong";
import { DetailedIdentity } from "./latency";

export const selectNodeSummaryErrors = createSelector(
  selectNodeRequestStatus,
  selectLivenessRequestStatus,
  (nodes, liveness) => [nodes.lastError, liveness.lastError],
);

export const selectNetworkLatencyState = (state: AdminUIState) =>
  state.cachedData.networkLatency;

// structure is e.g.:
// {
//  1: {
//    latencies: {
//      2: 100,
//      3: 200,
//    },
//    liveness: 0,
//  },
// }
export const selectNodeConnectivity = createSelector(
  selectNetworkLatencyState,
  state => state.data?.connectivity_by_node_id,
);

export const selectLastError = createSelector(
  selectNetworkLatencyState,
  state => state.lastError,
);

// selectValidLatencies constructs an array of latencies
// that are non-negative. These values are to be used for
// standard deviation related calculations.
export const selectValidLatencies = createSelector(
  selectNodeConnectivity,
  nodeConnectivity => {
    const latencies: number[] = [];
    _.forEach(nodeConnectivity, connectivity => {
      _.forEach(connectivity?.latencies, latency => {
        if (latency >= 0) {
          latencies.push(util.NanoToMilli(latency));
        }
      });
    });
    return latencies;
  },
);

// selectLiveNodes returns the node ids that have that active membership status.
export const selectLiveNodeIDs = createSelector(
  selectNodeConnectivity,
  nodeConnectivity => {
    const nodes: number[] = [];
    _.map(nodeConnectivity, (v, k) => {
      if (v.liveness?.membership === MembershipStatus.ACTIVE) {
        nodes.push(parseInt(k));
      }
    });
    return nodes;
  },
);

// selectNonLiveNodes returns node ids that don't have active membership status.
export const selectNonLiveNodeIDs = createSelector(
  selectNodeConnectivity,
  nodeConnectivity => {
    const nodes: number[] = [];
    _.map(nodeConnectivity, (v, k) => {
      if (
        v.liveness?.membership === MembershipStatus.DECOMMISSIONED ||
        v.liveness?.membership === MembershipStatus.DECOMMISSIONING
      ) {
        nodes.push(parseInt(k));
      }
    });
    return nodes;
  },
);

// createIdentityArray constructs an array of Identity.
export const selectIdentityArray = createSelector(
  nodesSummarySelector,
  nodesSummary => {
    const identities: Identity[] = [];
    _.map(nodesSummary.nodeStatuses, status => {
      identities.push({
        nodeID: status.desc.node_id,
        address: status.desc.address.address_field,
        locality: localityToString(status.desc.locality),
        updatedAt: util.LongToMoment(status.updated_at),
      });
    });
    return identities;
  },
);

// selectNoConnectionNodes returns an array of NoConnectiones
// that is determined by latencies returned either being a
// negative value or having no entry.
export const selectNoConnectionNodes = createSelector(
  selectNodeConnectivity,
  selectIdentityArray,
  (nodeConnectivity, identities) => {
    const noConnections: NoConnection[] = [];
    if (!nodeConnectivity || !identities) {
      return noConnections;
    }
    const nodeIDs = identities.map(identity => identity.nodeID);
    identities.forEach(identity => {
      const nodeID = identity.nodeID;
      const latencyMap = nodeConnectivity.latencies[nodeID.toString()];
      // First check for non entries that aren't the current nodeID.
      nodeIDs
        .filter(id => id !== nodeID)
        .map(id => {
          if (!latencyMap[id.toString()]) {
            noConnections.push({
              from: identity,
              to: identities.find(toIdentity => toIdentity.nodeID === id),
            });
          }
        });
      // Next, check for any latencies less than 0.
      _.map(latencyMap, (v, k) => {
        if (v < 0) {
          noConnections.push({
            from: identity,
            to: identities.find(
              toIdentity => toIdentity.nodeID === parseInt(k),
            ),
          });
        }
      });
    });
    return noConnections;
  },
);

// createDetailedIdentityArray is a method to construct DetailedIdentity
// objects from a filtered Identity array and a latencies map. It needs
// values passed in instead of taking directly from the store due to
// sorting and filtering which may result in fewer identities.
export const createDetailedIdentityArray = (
  identities: Identity[],
  networkMap: { [x: string]: any },
  multipleHeader: boolean,
  nodeId: string,
): [DetailedIdentity[]] => {
  const data: any = [];
  let rowLength = 0;
  const filteredData = identities.map(identityA => {
    const row: any[] = [];
    identities.forEach(identityB => {
      const a = networkMap[identityA.toString()]?.latencies || {};
      const latency = a[identityB.toString()] || null;
      // The cases listed below are:
      // if the identities are the same, if there was no
      // latency entry (meaning never attempted connection),
      // if there is a negative latency (meaning currently no
      // connection), or a normal postive latency.
      if (identityA.nodeID === identityB.nodeID) {
        row.push({ latency: 0, identityB });
      } else if (latency === null) {
        row.push({ latency: -2, identityB });
      } else if (latency < 0) {
        row.push({ latency: -1, identityB });
      } else {
        const milliLatency = util.NanoToMilli(latency);
        row.push({ milliLatency, identityB });
      }
    });
    rowLength = row.length;
    return { row, ...identityA };
  });
  // This block creates multiple arrays in data if the titles are different.
  filteredData.forEach(value => {
    const newValue = {
      ...value,
      title: multipleHeader
        ? getValueFromString(nodeId, value.locality)
        : value.locality,
    };
    if (data.length === 0) {
      data[0] = new Array(newValue);
    } else {
      if (data[data.length - 1][0].title === newValue.title) {
        data[data.length - 1].push(newValue);
        data[data.length - 1][0].rowCount = data[data.length - 1].length;
      } else {
        data[data.length] = new Array(newValue);
      }
    }
  });
  return data;
};

// @santamaura to remove once NetworkConnectivity endpoint is available.
export const nodesSummaryLatencies = createSelector(
  nodesSummarySelector,
  nodesSummary => {
    const healthyIDs = _.chain(nodesSummary.nodeIDs)
      .filter(
        nodeID =>
          nodesSummary.livenessStatusByNodeID[nodeID] ===
          LivenessStatus.NODE_STATUS_LIVE,
      )
      .filter(nodeID => !_.isNil(nodesSummary.nodeStatusByID[nodeID].activity))
      .map(nodeID => Number.parseInt(nodeID, 0))
      .value();
    return _.flatMap(healthyIDs, nodeIDa =>
      _.chain(healthyIDs)
        .without(nodeIDa)
        .map(nodeIDb => nodesSummary.nodeStatusByID[nodeIDa].activity[nodeIDb])
        .filter(activity => !_.isNil(activity) && !_.isNil(activity.latency))
        .map(activity => util.NanoToMilli(FixLong(activity.latency).toNumber()))
        .filter(ms => _.isFinite(ms) && ms > 0)
        .value(),
    );
  },
);

export const nodesSummaryHealthyIDs = createSelector(
  nodesSummarySelector,
  nodesSummary => {
    return _.chain(nodesSummary.nodeIDs)
      .filter(
        nodeID =>
          nodesSummary.livenessStatusByNodeID[nodeID] ===
          LivenessStatus.NODE_STATUS_LIVE,
      )
      .filter(nodeID => !_.isNil(nodesSummary.nodeStatusByID[nodeID].activity))
      .map(nodeID => Number.parseInt(nodeID, 0));
  },
);

export const nodesSummaryStaleIDs = createSelector(
  nodesSummarySelector,
  nodesSummary => {
    return _.chain(nodesSummary.nodeIDs)
      .filter(
        nodeID =>
          nodesSummary.livenessStatusByNodeID[nodeID] ===
          LivenessStatus.NODE_STATUS_UNAVAILABLE,
      )
      .map(nodeID => Number.parseInt(nodeID, 0));
  },
);
