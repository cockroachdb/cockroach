// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { MetricConstants, INodeStatus } from "src/util/proto";

import {
  LivenessStatus,
  sumNodeStats,
  getDisplayName,
  getNumNodesByVersionsTag,
  validateNodes,
} from "./nodes";

describe("sumNodeStats", function () {
  // Each of these nodes only has half of its capacity "usable" for cockroach data.
  // See diagram for what these stats mean:
  // https://github.com/cockroachdb/cockroach/blob/31e4299ab73a43f539b1ba63ed86be5ee18685f6/pkg/storage/metrics.go#L145-L153
  const nodeStatuses: INodeStatus[] = [
    {
      desc: { node_id: 1 },
      metrics: {
        [MetricConstants.capacity]: 100,
        [MetricConstants.usedCapacity]: 10,
        [MetricConstants.availableCapacity]: 40,
      },
    },
    {
      desc: { node_id: 2 },
      metrics: {
        [MetricConstants.capacity]: 100,
        [MetricConstants.usedCapacity]: 10,
        [MetricConstants.availableCapacity]: 40,
      },
    },
  ];

  it("sums stats from an array of nodes", function () {
    const livenessStatusByNodeID: { [key: string]: LivenessStatus } = {
      1: LivenessStatus.NODE_STATUS_LIVE,
      2: LivenessStatus.NODE_STATUS_LIVE,
    };
    const actual = sumNodeStats(nodeStatuses, livenessStatusByNodeID);
    expect(actual.nodeCounts.healthy).toBe(2);
    expect(actual.capacityTotal).toBe(200);
    expect(actual.capacityUsed).toBe(20);
    // usable = used + available.
    expect(actual.capacityUsable).toBe(100);
  });

  it("returns empty stats if liveness statuses are not provided", () => {
    const { nodeCounts, ...restStats } = sumNodeStats(nodeStatuses, {});
    Object.entries(restStats).forEach(([_, value]) => expect(value).toBe(0));
    Object.entries(nodeCounts).forEach(([_, value]) => expect(value).toBe(0));
  });
});

describe("getDisplayName", function () {
  it("returns node id appended to address", function () {
    const node: INodeStatus = {
      desc: {
        node_id: 1,
        address: { address_field: "addressA" },
      },
    };
    expect(getDisplayName(node)).toEqual("(n1) addressA");
  });

  it("returns node id without address when includeAddress is false", function () {
    const node: INodeStatus = {
      desc: {
        node_id: 1,
        address: { address_field: "addressA" },
      },
    };
    expect(
      getDisplayName(node, LivenessStatus.NODE_STATUS_LIVE, false),
    ).toEqual("n1");
  });

  it("adds decommissioned flag for decommissioned nodes", function () {
    const node: INodeStatus = {
      desc: {
        node_id: 1,
        address: { address_field: "addressA" },
      },
    };
    expect(
      getDisplayName(node, LivenessStatus.NODE_STATUS_DECOMMISSIONED),
    ).toEqual("[decommissioned] (n1) addressA");
  });
});

describe("getNumNodesByVersionsTag", () => {
  it("correctly returns the different binary versions and the number of associated nodes", () => {
    const nodes: INodeStatus[] = [
      {
        desc: { node_id: 1 },
        build_info: { tag: "v22.1" },
      },
      {
        desc: { node_id: 2 },
        build_info: { tag: "v22.1" },
      },
      {
        desc: { node_id: 3 },
        build_info: { tag: "v21.1.7" },
      },
    ];
    const expectedResult = new Map([
      ["v22.1", 2],
      ["v21.1.7", 1],
    ]);
    expect(getNumNodesByVersionsTag(nodes)).toEqual(expectedResult);
  });

  it("returns empty map for undefined nodes", () => {
    expect(getNumNodesByVersionsTag(undefined)).toEqual(new Map());
  });
});

describe("validateNodes", function () {
  it("returns undefined for undefined nodeStatuses", function () {
    expect(validateNodes(undefined, {})).toBeUndefined();
  });

  it("filters nodes without build_info", function () {
    const nodes: INodeStatus[] = [
      {
        desc: { node_id: 1 },
        build_info: { tag: "v22.1" },
      },
      {
        desc: { node_id: 2 },
        // no build_info
      },
    ];
    const result = validateNodes(nodes, {});
    expect(result.length).toBe(1);
    expect(result[0].desc.node_id).toBe(1);
  });
});
