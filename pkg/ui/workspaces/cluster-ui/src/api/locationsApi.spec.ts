// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";

import { buildLocalityTree } from "./locationsApi";

type INodeStatus = cockroach.server.status.statuspb.INodeStatus;

interface Tier {
  key: string;
  value: string;
}

function makeNode(nodeId: number, tiers: Tier[]): INodeStatus {
  return {
    desc: {
      node_id: nodeId,
      address: { address_field: `addr${nodeId}:26257` },
      locality: { tiers },
    },
  } as INodeStatus;
}

describe("buildLocalityTree", () => {
  it("returns an empty tree when given no nodes", () => {
    const tree = buildLocalityTree([]);

    expect(tree.tiers).toEqual([]);
    expect(tree.nodes).toEqual([]);
    expect(tree.localities).toEqual({});
  });

  it("returns an empty tree when called with no arguments", () => {
    const tree = buildLocalityTree();

    expect(tree.tiers).toEqual([]);
    expect(tree.nodes).toEqual([]);
    expect(tree.localities).toEqual({});
  });

  it("places nodes without locality tiers at the top level", () => {
    const node = makeNode(1, []);
    const tree = buildLocalityTree([node]);

    expect(tree.tiers).toEqual([]);
    expect(tree.nodes).toEqual([node]);
    expect(tree.localities).toEqual({});
  });

  it("groups nodes by a single locality tier", () => {
    const n1 = makeNode(1, [{ key: "region", value: "us-east-1" }]);
    const n2 = makeNode(2, [{ key: "region", value: "us-east-2" }]);
    const tree = buildLocalityTree([n1, n2]);

    expect(tree.tiers).toEqual([]);
    expect(tree.nodes).toEqual([]);

    const regions = tree.localities.region;
    expect(Object.keys(regions)).toEqual(
      expect.arrayContaining(["us-east-1", "us-east-2"]),
    );

    const usEast1 = regions["us-east-1"];
    expect(usEast1.tiers).toEqual([{ key: "region", value: "us-east-1" }]);
    expect(usEast1.nodes).toEqual([n1]);
    expect(usEast1.localities).toEqual({});

    const usEast2 = regions["us-east-2"];
    expect(usEast2.tiers).toEqual([{ key: "region", value: "us-east-2" }]);
    expect(usEast2.nodes).toEqual([n2]);
    expect(usEast2.localities).toEqual({});
  });

  it("groups nodes with the same locality value together", () => {
    const n1 = makeNode(1, [{ key: "region", value: "us-east-1" }]);
    const n2 = makeNode(2, [{ key: "region", value: "us-east-1" }]);
    const tree = buildLocalityTree([n1, n2]);

    const usEast1 = tree.localities.region["us-east-1"];
    expect(usEast1.nodes).toEqual([n1, n2]);
  });

  it("handles multiple levels of locality tiers", () => {
    const n1 = makeNode(1, [
      { key: "region", value: "us-east-1" },
      { key: "az", value: "a" },
    ]);
    const n2 = makeNode(2, [
      { key: "region", value: "us-east-1" },
      { key: "az", value: "b" },
    ]);
    const tree = buildLocalityTree([n1, n2]);

    // Top level has no nodes, only locality grouping by region.
    expect(tree.nodes).toEqual([]);
    const usEast1 = tree.localities.region["us-east-1"];

    expect(usEast1.tiers).toEqual([{ key: "region", value: "us-east-1" }]);
    expect(usEast1.nodes).toEqual([]);

    // Second level groups by az.
    const azA = usEast1.localities.az["a"];
    expect(azA.tiers).toEqual([
      { key: "region", value: "us-east-1" },
      { key: "az", value: "a" },
    ]);
    expect(azA.nodes).toEqual([n1]);

    const azB = usEast1.localities.az["b"];
    expect(azB.tiers).toEqual([
      { key: "region", value: "us-east-1" },
      { key: "az", value: "b" },
    ]);
    expect(azB.nodes).toEqual([n2]);
  });

  it("handles mixed tier depths", () => {
    // n1 has only a region tier, n2 has region + az.
    const n1 = makeNode(1, [{ key: "region", value: "us-east-1" }]);
    const n2 = makeNode(2, [
      { key: "region", value: "us-east-1" },
      { key: "az", value: "a" },
    ]);
    const tree = buildLocalityTree([n1, n2]);

    const usEast1 = tree.localities.region["us-east-1"];
    // n1 stops at region depth, so it's a direct node of us-east-1.
    expect(usEast1.nodes).toEqual([n1]);
    // n2 goes deeper into az.
    expect(usEast1.localities.az["a"].nodes).toEqual([n2]);
  });

  it("separates nodes across different locality keys", () => {
    const n1 = makeNode(1, [{ key: "region", value: "us-east-1" }]);
    const n2 = makeNode(2, [{ key: "dc", value: "dc-1" }]);
    const tree = buildLocalityTree([n1, n2]);

    expect(tree.localities.region["us-east-1"].nodes).toEqual([n1]);
    expect(tree.localities.dc["dc-1"].nodes).toEqual([n2]);
  });

  it("handles three levels of nesting", () => {
    const n1 = makeNode(1, [
      { key: "region", value: "us-east-1" },
      { key: "az", value: "a" },
      { key: "rack", value: "1" },
    ]);
    const tree = buildLocalityTree([n1]);

    const rack =
      tree.localities.region["us-east-1"].localities.az["a"].localities.rack[
        "1"
      ];
    expect(rack.nodes).toEqual([n1]);
    expect(rack.tiers).toEqual([
      { key: "region", value: "us-east-1" },
      { key: "az", value: "a" },
      { key: "rack", value: "1" },
    ]);
  });
});
