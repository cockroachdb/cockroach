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
import { nodesSummarySelector } from "src/redux/nodes";
import { Identity } from ".";
import { localityToString } from "../../components/nodeFilterList";

import MembershipStatus = cockroach.kv.kvserver.liveness.livenesspb.MembershipStatus;

export const selectNetworkLatencyState = (state: AdminUIState) =>
  state.cachedData.networkLatency;

// structure is e.g.:
// latencies: {
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
      _.forEach(connectivity.latencies, latency => {
        if (latency >= 0) {
          latencies.push(util.NanoToMilli(latency));
        }
      });
    });
    return latencies;
  },
);

// selectNodesByMembership receives a membership status and returns the node
// ids that have that membership status.
export const selectNodesByMembership = (membership: typeof MembershipStatus) =>
  createSelector(selectNodeConnectivity, nodeConnectivity => {
    const nodes: number[] = [];
    _.map(nodeConnectivity, (v, k) => {
      if (v.liveness?.membership === membership) {
        nodes.push(parseInt(k));
      }
    });
    return nodes;
  });

// createIdentityArray constructs an array of Identity.
export const createIdentityArray = createSelector(
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
