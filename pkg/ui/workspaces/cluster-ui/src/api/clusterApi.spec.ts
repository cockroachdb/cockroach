// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { renderHook } from "@testing-library/react-hooks";

import {
  getClusterName,
  getClusterVersionLabel,
  useClusterLabel,
} from "./clusterApi";
import * as livenessApi from "./livenessApi";
import * as nodesApi from "./nodesApi";

type INodeStatus = cockroach.server.status.statuspb.INodeStatus;
type ILiveness = cockroach.kv.kvserver.liveness.livenesspb.ILiveness;
const LivenessStatus =
  cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus;
const MembershipStatus =
  cockroach.kv.kvserver.liveness.livenesspb.MembershipStatus;

function makeNodeStatus(
  nodeId: number,
  opts: { tag?: string; clusterName?: string } = {},
): INodeStatus {
  return {
    desc: {
      node_id: nodeId,
      cluster_name: opts.clusterName,
    },
    build_info: opts.tag != null ? { tag: opts.tag } : undefined,
  } as INodeStatus;
}

function makeLiveness(
  nodeId: number,
  membership?: cockroach.kv.kvserver.liveness.livenesspb.MembershipStatus,
): ILiveness {
  return { node_id: nodeId, membership } as ILiveness;
}

describe("getClusterName", () => {
  it("returns undefined for empty node statuses", () => {
    expect(getClusterName([], {})).toBeUndefined();
  });

  it("returns undefined for null node statuses", () => {
    expect(getClusterName(null, {})).toBeUndefined();
  });

  it("returns undefined when liveness statuses are empty", () => {
    const nodes = [makeNodeStatus(1, { clusterName: "my-cluster" })];
    expect(getClusterName(nodes, {})).toBeUndefined();
  });

  it("returns undefined when no nodes are live", () => {
    const nodes = [makeNodeStatus(1, { clusterName: "my-cluster" })];
    expect(
      getClusterName(nodes, { "1": LivenessStatus.NODE_STATUS_DEAD }),
    ).toBeUndefined();
  });

  it("returns cluster name from a live node", () => {
    const nodes = [makeNodeStatus(1, { clusterName: "my-cluster" })];
    expect(
      getClusterName(nodes, { "1": LivenessStatus.NODE_STATUS_LIVE }),
    ).toBe("my-cluster");
  });

  it("skips live nodes without a cluster name", () => {
    const nodes = [
      makeNodeStatus(1),
      makeNodeStatus(2, { clusterName: "found-it" }),
    ];
    expect(
      getClusterName(nodes, {
        "1": LivenessStatus.NODE_STATUS_LIVE,
        "2": LivenessStatus.NODE_STATUS_LIVE,
      }),
    ).toBe("found-it");
  });

  it("returns undefined when no live nodes have a cluster name", () => {
    const nodes = [makeNodeStatus(1), makeNodeStatus(2)];
    expect(
      getClusterName(nodes, {
        "1": LivenessStatus.NODE_STATUS_LIVE,
        "2": LivenessStatus.NODE_STATUS_LIVE,
      }),
    ).toBeUndefined();
  });
});

describe("getClusterVersionLabel", () => {
  it("returns undefined for null node statuses", () => {
    expect(getClusterVersionLabel(null, {})).toBeUndefined();
  });

  it("returns undefined when no nodes have build info", () => {
    const nodes = [makeNodeStatus(1)]; // no tag => no build_info
    expect(getClusterVersionLabel(nodes, {})).toBeUndefined();
  });

  it("returns single version when all nodes run the same version", () => {
    const nodes = [
      makeNodeStatus(1, { tag: "v23.1.0" }),
      makeNodeStatus(2, { tag: "v23.1.0" }),
    ];
    expect(getClusterVersionLabel(nodes, {})).toBe("v23.1.0");
  });

  it("returns lowest version with suffix for mixed versions", () => {
    const nodes = [
      makeNodeStatus(1, { tag: "v23.2.0" }),
      makeNodeStatus(2, { tag: "v23.1.0" }),
    ];
    expect(getClusterVersionLabel(nodes, {})).toBe("v23.1.0 - Mixed Versions");
  });

  it("includes nodes with ACTIVE membership (value 0)", () => {
    const nodes = [
      makeNodeStatus(1, { tag: "v23.1.0" }),
      makeNodeStatus(2, { tag: "v23.2.0" }),
    ];
    const liveness: Record<string, ILiveness> = {
      "1": makeLiveness(1, MembershipStatus.ACTIVE),
      "2": makeLiveness(2, MembershipStatus.ACTIVE),
    };
    expect(getClusterVersionLabel(nodes, liveness)).toBe(
      "v23.1.0 - Mixed Versions",
    );
  });

  it("excludes decommissioning nodes from version calculation", () => {
    const nodes = [
      makeNodeStatus(1, { tag: "v23.1.0" }),
      makeNodeStatus(2, { tag: "v23.2.0" }),
    ];
    const liveness: Record<string, ILiveness> = {
      "1": makeLiveness(1, MembershipStatus.ACTIVE),
      "2": makeLiveness(2, MembershipStatus.DECOMMISSIONING),
    };
    // Node 2 excluded; only v23.1.0 remains.
    expect(getClusterVersionLabel(nodes, liveness)).toBe("v23.1.0");
  });

  it("excludes decommissioned nodes from version calculation", () => {
    const nodes = [
      makeNodeStatus(1, { tag: "v23.1.0" }),
      makeNodeStatus(2, { tag: "v23.2.0" }),
    ];
    const liveness: Record<string, ILiveness> = {
      "1": makeLiveness(1, MembershipStatus.ACTIVE),
      "2": makeLiveness(2, MembershipStatus.DECOMMISSIONED),
    };
    expect(getClusterVersionLabel(nodes, liveness)).toBe("v23.1.0");
  });

  it("includes nodes with no liveness record", () => {
    const nodes = [
      makeNodeStatus(1, { tag: "v23.1.0" }),
      makeNodeStatus(2, { tag: "v23.1.0" }),
    ];
    // No liveness entries at all.
    expect(getClusterVersionLabel(nodes, {})).toBe("v23.1.0");
  });

  it("includes nodes with null membership", () => {
    const nodes = [
      makeNodeStatus(1, { tag: "v23.1.0" }),
      makeNodeStatus(2, { tag: "v23.2.0" }),
    ];
    const liveness: Record<string, ILiveness> = {
      "1": makeLiveness(1, null),
      "2": makeLiveness(2, null),
    };
    expect(getClusterVersionLabel(nodes, liveness)).toBe(
      "v23.1.0 - Mixed Versions",
    );
  });

  it("returns undefined when all nodes are decommissioned", () => {
    const nodes = [
      makeNodeStatus(1, { tag: "v23.1.0" }),
      makeNodeStatus(2, { tag: "v23.2.0" }),
    ];
    const liveness: Record<string, ILiveness> = {
      "1": makeLiveness(1, MembershipStatus.DECOMMISSIONED),
      "2": makeLiveness(2, MembershipStatus.DECOMMISSIONED),
    };
    expect(getClusterVersionLabel(nodes, liveness)).toBeUndefined();
  });
});

describe("useClusterLabel", () => {
  let spyUseNodes: jest.SpyInstance;
  let spyUseLiveness: jest.SpyInstance;

  beforeEach(() => {
    spyUseNodes = jest.spyOn(nodesApi, "useNodes");
    spyUseLiveness = jest.spyOn(livenessApi, "useLiveness");
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("returns cluster name and version from live nodes", () => {
    spyUseNodes.mockReturnValue({
      nodeStatuses: [
        makeNodeStatus(1, { tag: "v23.1.0", clusterName: "my-cluster" }),
        makeNodeStatus(2, { tag: "v23.1.0" }),
      ],
      isLoading: false,
      error: undefined,
    });
    spyUseLiveness.mockReturnValue({
      livenesses: [
        makeLiveness(1, MembershipStatus.ACTIVE),
        makeLiveness(2, MembershipStatus.ACTIVE),
      ],
      statuses: {
        "1": LivenessStatus.NODE_STATUS_LIVE,
        "2": LivenessStatus.NODE_STATUS_LIVE,
      },
      isLoading: false,
      error: undefined,
    });

    const { result } = renderHook(() => useClusterLabel());
    expect(result.current.clusterName).toBe("my-cluster");
    expect(result.current.clusterVersion).toBe("v23.1.0");
    expect(result.current.isLoading).toBe(false);
    expect(result.current.error).toBeUndefined();
  });

  it("excludes decommissioning nodes from version but not cluster name", () => {
    spyUseNodes.mockReturnValue({
      nodeStatuses: [
        makeNodeStatus(1, { tag: "v23.1.0", clusterName: "my-cluster" }),
        makeNodeStatus(2, { tag: "v23.2.0" }),
      ],
      isLoading: false,
      error: undefined,
    });
    spyUseLiveness.mockReturnValue({
      livenesses: [
        makeLiveness(1, MembershipStatus.ACTIVE),
        makeLiveness(2, MembershipStatus.DECOMMISSIONING),
      ],
      statuses: {
        "1": LivenessStatus.NODE_STATUS_LIVE,
        "2": LivenessStatus.NODE_STATUS_LIVE,
      },
      isLoading: false,
      error: undefined,
    });

    const { result } = renderHook(() => useClusterLabel());
    expect(result.current.clusterVersion).toBe("v23.1.0");
  });

  it("is loading when either hook is loading", () => {
    spyUseNodes.mockReturnValue({
      nodeStatuses: [],
      isLoading: true,
      error: undefined,
    });
    spyUseLiveness.mockReturnValue({
      livenesses: [],
      statuses: {},
      isLoading: false,
      error: undefined,
    });

    const { result } = renderHook(() => useClusterLabel());
    expect(result.current.isLoading).toBe(true);
  });

  it("returns first available error", () => {
    const nodesErr = new Error("nodes failed");
    spyUseNodes.mockReturnValue({
      nodeStatuses: [],
      isLoading: false,
      error: nodesErr,
    });
    spyUseLiveness.mockReturnValue({
      livenesses: [],
      statuses: {},
      isLoading: false,
      error: undefined,
    });

    const { result } = renderHook(() => useClusterLabel());
    expect(result.current.error).toBe(nodesErr);
  });
});
