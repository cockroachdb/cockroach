// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { NodeStatus } from "src/api";
import { NodeID, StoreID } from "src/types/clusterTypes";

// mapStoreIDsToNodeRegions creates a mapping of regions
// to the nodes that are present in that region based on
// the provided storeIDs.
export const mapStoreIDsToNodeRegions = (
  stores: StoreID[],
  clusterNodeIDToRegion: Record<NodeID, NodeStatus> = {},
  clusterStoreIDToNodeID: Record<StoreID, NodeID> = {},
): Record<string, NodeID[]> => {
  const nodes = stores.reduce((acc, storeID) => {
    acc.add(clusterStoreIDToNodeID[storeID]);
    return acc;
  }, new Set<NodeID>());

  const nodesByRegion: Record<string, NodeID[]> = {};
  nodes.forEach(nodeID => {
    const region = clusterNodeIDToRegion[nodeID]?.region;
    if (!nodesByRegion[region]) {
      nodesByRegion[region] = [];
    }
    nodesByRegion[region].push(nodeID);
  });

  // Sort nodes.
  Object.keys(nodesByRegion).forEach(region => {
    nodesByRegion[region].sort();
  });

  return nodesByRegion;
};
