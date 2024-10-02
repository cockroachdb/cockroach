// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createMemoryHistory } from "history";
import merge from "lodash/merge";

import { AdminUIState, createAdminUIStore } from "src/redux/state";

import {
  selectLocalityTree,
  LocalityTier,
  selectNodeLocalities,
} from "./localities";

function makeStateWithLocalities(localities: LocalityTier[][]): AdminUIState {
  const nodes = localities.map((locality, i) => {
    return {
      desc: {
        node_id: i,
        locality: locality ? { tiers: locality } : {},
      },
    };
  });
  const store = createAdminUIStore(createMemoryHistory());
  return merge<AdminUIState, RecursivePartial<AdminUIState>>(store.getState(), {
    cachedData: {
      nodes: {
        data: nodes,
        inFlight: false,
        valid: true,
        unauthorized: false,
      },
      liveness: {},
    },
  });
}

describe("selectLocalityTree", function () {
  it("puts nodes without locality at the top-level", function () {
    const state = makeStateWithLocalities([[]]);

    const tree = selectLocalityTree(state);

    expect(tree.tiers).toEqual([]);
    expect(tree.localities).toEqual({});

    expect(tree.nodes.length).toBe(1);
  });

  it("organizes nodes by locality", function () {
    const state = makeStateWithLocalities([
      [{ key: "region", value: "us-east-1" }],
      [{ key: "region", value: "us-east-2" }],
    ]);

    const tree = selectLocalityTree(state);

    expect(tree.tiers).toEqual([]);
    expect(tree.nodes).toEqual([]);

    expect(Object.keys(tree.localities)).toContain("region");
    const regions = tree.localities.region;

    expect(Object.keys(regions)).toContain("us-east-1");
    expect(Object.keys(regions)).toContain("us-east-2");

    const usEast1 = regions["us-east-1"];

    expect(usEast1.localities).toEqual({});
    expect(usEast1.tiers).toEqual([{ key: "region", value: "us-east-1" }]);

    expect(usEast1.nodes.length).toBe(1);

    const usEast2 = regions["us-east-2"];

    expect(usEast2.localities).toEqual({});
    expect(usEast2.tiers).toEqual([{ key: "region", value: "us-east-2" }]);

    expect(usEast2.nodes.length).toBe(1);
  });
});

describe("selectNodeLocalities", function () {
  it("should return map of nodes with localities", function () {
    const localities = [
      [
        { key: "region", value: "us-east-1" },
        { key: "az", value: "a" },
      ],
      [{ key: "region", value: "us-east-2" }],
    ];
    const state = makeStateWithLocalities(localities);

    const result = selectNodeLocalities.resultFunc(state.cachedData.nodes.data);
    expect(result.size).toBe(2);
    result.forEach((v, k) => {
      expect(v).toEqual(
        localities[k].map(l => `${l.key}=${l.value}`).join(", "),
      );
    });
  });

  it("should return empty map if no locality is provided", function () {
    const state = makeStateWithLocalities([]);
    const result = selectNodeLocalities.resultFunc(state.cachedData.nodes.data);
    expect(result.size).toBe(0);
  });
});
