// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { INodeStatus } from "src/util/proto";

import { buildLocalityTree, LocalityTier } from "./localities";

function makeNodes(localities: LocalityTier[][]): INodeStatus[] {
  return localities.map((locality, i) => {
    return {
      desc: {
        node_id: i,
        locality: locality ? { tiers: locality } : {},
      },
    };
  });
}

describe("buildLocalityTree", function () {
  it("puts nodes without locality at the top-level", function () {
    const nodes = makeNodes([[]]);

    const tree = buildLocalityTree(nodes);

    expect(tree.tiers).toEqual([]);
    expect(tree.localities).toEqual({});

    expect(tree.nodes.length).toBe(1);
  });

  it("organizes nodes by locality", function () {
    const nodes = makeNodes([
      [{ key: "region", value: "us-east-1" }],
      [{ key: "region", value: "us-east-2" }],
    ]);

    const tree = buildLocalityTree(nodes);

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
