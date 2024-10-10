// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { StoreID } from "src/types/clusterTypes";

import { mapStoreIDsToNodeRegions } from "./nodeUtils";

describe("nodeUtils", () => {
  describe("mapStoreIDsToNodeRegions", () => {
    it("should return a mapping of regions to the nodes that are present in that region based on the provided storeIDs", () => {
      const stores = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] as StoreID[];
      const clusterNodeIDToRegion = {
        1: "region1",
        2: "region2",
        3: "region1",
        4: "region2",
        5: "region1",
      };
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
