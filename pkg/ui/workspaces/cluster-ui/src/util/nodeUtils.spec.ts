// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { NodeStatus } from "src/api";
import { NodeID, StoreID } from "src/types/clusterTypes";

import { mapStoreIDsToNodeRegions } from "./nodeUtils";

describe("nodeUtils", () => {
  describe("mapStoreIDsToNodeRegions", () => {
    it("should return a mapping of regions to the nodes that are present in that region based on the provided storeIDs", () => {
      const stores = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] as StoreID[];
      const clusterNodeIDToRegion = {
        1: { region: "region1", stores: [1, 6] },
        2: { region: "region2", stores: [2, 7] },
        3: { region: "region1", stores: [3, 8] },
        4: { region: "region2", stores: [4, 9] },
        5: { region: "region1", stores: [5, 10] },
      } as Record<NodeID, NodeStatus>;
      const clusterStoreIDToNodeID = {
        1: 1,
        2: 2,
        3: 3,
        4: 4,
        5: 5,
        6: 1,
        7: 2,
        8: 3,
        9: 4,
        10: 5,
      };

      const result = mapStoreIDsToNodeRegions(
        stores,
        clusterNodeIDToRegion,
        clusterStoreIDToNodeID,
      );

      expect(result).toEqual({
        region1: [1, 3, 5],
        region2: [2, 4],
      });
    });
  });
});
