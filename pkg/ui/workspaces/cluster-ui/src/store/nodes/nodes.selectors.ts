// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createSelector } from "@reduxjs/toolkit";
import _ from "lodash";
import { AppState } from "../reducers";
import { getDisplayName } from "../../nodes";
import { livenessStatusByNodeIDSelector } from "../liveness";
import { accumulateMetrics } from "src/util/proto";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
type ILocality = cockroach.roachpb.ILocality;

export const nodeStatusesSelector = (state: AppState) =>
  state.adminUI?.nodes.data || [];

export const nodesSelector = createSelector(
  nodeStatusesSelector,
  accumulateMetrics,
);

export const nodeDisplayNameByIDSelector = createSelector(
  nodesSelector,
  livenessStatusByNodeIDSelector,
  (nodeStatuses, livenessStatusByNodeID) => {
    const result: { [key: string]: string } = {};
    if (!_.isEmpty(nodeStatuses)) {
      nodeStatuses.forEach(ns => {
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
  nodeStatuses => {
    const result: { [key: string]: string } = {};
    if (!_.isEmpty(nodeStatuses)) {
      nodeStatuses.forEach(ns => {
        result[ns.desc.node_id] = getRegionFromLocality(ns.desc.locality);
      });
    }
    return result;
  },
);
