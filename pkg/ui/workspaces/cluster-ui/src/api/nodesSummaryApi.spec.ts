// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { renderHook } from "@testing-library/react-hooks";

import * as livenessApi from "./livenessApi";
import * as nodesApi from "./nodesApi";
import { useNodesSummary } from "./nodesSummaryApi";

type INodeStatus = cockroach.server.status.statuspb.INodeStatus;
type ILiveness = cockroach.kv.kvserver.liveness.livenesspb.ILiveness;
const LivenessStatus =
  cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus;

function makeNodeStatus(
  nodeId: number,
  address: string,
  storeIds: number[] = [],
): INodeStatus {
  return {
    desc: {
      node_id: nodeId,
      address: { address_field: address },
      locality: { tiers: [] },
    },
    store_statuses: storeIds.map(sid => ({
      desc: { store_id: sid },
    })),
  } as INodeStatus;
}

function makeLiveness(nodeId: number): ILiveness {
  return { node_id: nodeId } as ILiveness;
}

describe("useNodesSummary", () => {
  let spyUseNodes: jest.SpyInstance;
  let spyUseLiveness: jest.SpyInstance;

  beforeEach(() => {
    spyUseNodes = jest.spyOn(nodesApi, "useNodes");
    spyUseLiveness = jest.spyOn(livenessApi, "useLiveness");
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("should return empty summary when both hooks return empty data", () => {
    spyUseNodes.mockReturnValue({
      nodeStatuses: [],
      isLoading: false,
      error: undefined,
    });
    spyUseLiveness.mockReturnValue({
      livenesses: [],
      statuses: {},
      isLoading: false,
      error: undefined,
    });

    const { result } = renderHook(() => useNodesSummary());
    expect(result.current.nodeStatuses).toEqual([]);
    expect(result.current.nodeIDs).toEqual([]);
    expect(result.current.nodeStatusByID).toEqual({});
    expect(result.current.nodeDisplayNameByID).toEqual({});
    expect(result.current.livenessStatusByNodeID).toEqual({});
    expect(result.current.livenessByNodeID).toEqual({});
    expect(result.current.storeIDsByNodeID).toEqual({});
    expect(result.current.isLoading).toBe(false);
    expect(result.current.error).toBeUndefined();
  });

  it("should build summary from node statuses and liveness data", () => {
    const ns1 = makeNodeStatus(1, "addr1:26257", [10, 11]);
    const ns2 = makeNodeStatus(2, "addr2:26257", [20]);

    spyUseNodes.mockReturnValue({
      nodeStatuses: [ns1, ns2],
      isLoading: false,
      error: undefined,
    });
    spyUseLiveness.mockReturnValue({
      livenesses: [makeLiveness(1), makeLiveness(2)],
      statuses: {
        "1": LivenessStatus.NODE_STATUS_LIVE,
        "2": LivenessStatus.NODE_STATUS_DEAD,
      },
      isLoading: false,
      error: undefined,
    });

    const { result } = renderHook(() => useNodesSummary());

    expect(result.current.nodeIDs).toEqual(["1", "2"]);
    expect(result.current.nodeStatusByID["1"]).toBe(ns1);
    expect(result.current.nodeStatusByID["2"]).toBe(ns2);
    expect(result.current.nodeDisplayNameByID["1"]).toBe("(n1) addr1:26257");
    expect(result.current.nodeDisplayNameByID["2"]).toBe("(n2) addr2:26257");
    expect(result.current.storeIDsByNodeID["1"]).toEqual(["10", "11"]);
    expect(result.current.storeIDsByNodeID["2"]).toEqual(["20"]);
    expect(result.current.livenessByNodeID["1"]).toEqual({ node_id: 1 });
    expect(result.current.livenessByNodeID["2"]).toEqual({ node_id: 2 });
    expect(result.current.livenessStatusByNodeID).toEqual({
      "1": LivenessStatus.NODE_STATUS_LIVE,
      "2": LivenessStatus.NODE_STATUS_DEAD,
    });
  });

  it("should show decommissioned prefix in display name", () => {
    const ns = makeNodeStatus(3, "addr3:26257");

    spyUseNodes.mockReturnValue({
      nodeStatuses: [ns],
      isLoading: false,
      error: undefined,
    });
    spyUseLiveness.mockReturnValue({
      livenesses: [makeLiveness(3)],
      statuses: {
        "3": LivenessStatus.NODE_STATUS_DECOMMISSIONED,
      },
      isLoading: false,
      error: undefined,
    });

    const { result } = renderHook(() => useNodesSummary());
    expect(result.current.nodeDisplayNameByID["3"]).toBe(
      "[decommissioned] (n3) addr3:26257",
    );
  });

  it("should skip node statuses with null node_id", () => {
    const valid = makeNodeStatus(1, "addr1:26257");
    const invalid = { desc: {} } as INodeStatus;

    spyUseNodes.mockReturnValue({
      nodeStatuses: [valid, invalid],
      isLoading: false,
      error: undefined,
    });
    spyUseLiveness.mockReturnValue({
      livenesses: [],
      statuses: {},
      isLoading: false,
      error: undefined,
    });

    const { result } = renderHook(() => useNodesSummary());
    expect(result.current.nodeIDs).toEqual(["1"]);
    expect(Object.keys(result.current.nodeStatusByID)).toEqual(["1"]);
  });

  it("should handle liveness entries with null node_id", () => {
    spyUseNodes.mockReturnValue({
      nodeStatuses: [],
      isLoading: false,
      error: undefined,
    });
    spyUseLiveness.mockReturnValue({
      livenesses: [{ node_id: null } as ILiveness, makeLiveness(5)],
      statuses: {},
      isLoading: false,
      error: undefined,
    });

    const { result } = renderHook(() => useNodesSummary());
    expect(result.current.livenessByNodeID).toEqual({ "5": { node_id: 5 } });
  });

  it.each([
    {
      name: "loading when nodes loading",
      nodesLoading: true,
      livenessLoading: false,
      expected: true,
    },
    {
      name: "loading when liveness loading",
      nodesLoading: false,
      livenessLoading: true,
      expected: true,
    },
    {
      name: "loading when both loading",
      nodesLoading: true,
      livenessLoading: true,
      expected: true,
    },
    {
      name: "not loading when neither loading",
      nodesLoading: false,
      livenessLoading: false,
      expected: false,
    },
  ])("$name", ({ nodesLoading, livenessLoading, expected }) => {
    spyUseNodes.mockReturnValue({
      nodeStatuses: [],
      isLoading: nodesLoading,
      error: undefined,
    });
    spyUseLiveness.mockReturnValue({
      livenesses: [],
      statuses: {},
      isLoading: livenessLoading,
      error: undefined,
    });

    const { result } = renderHook(() => useNodesSummary());
    expect(result.current.isLoading).toBe(expected);
  });

  it("should return nodes error when only nodes fails", () => {
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

    const { result } = renderHook(() => useNodesSummary());
    expect(result.current.error).toBe(nodesErr);
  });

  it("should return liveness error when only liveness fails", () => {
    const livenessErr = new Error("liveness failed");

    spyUseNodes.mockReturnValue({
      nodeStatuses: [],
      isLoading: false,
      error: undefined,
    });
    spyUseLiveness.mockReturnValue({
      livenesses: [],
      statuses: {},
      isLoading: false,
      error: livenessErr,
    });

    const { result } = renderHook(() => useNodesSummary());
    expect(result.current.error).toBe(livenessErr);
  });

  it("should prefer nodes error when both fail", () => {
    const nodesErr = new Error("nodes failed");
    const livenessErr = new Error("liveness failed");

    spyUseNodes.mockReturnValue({
      nodeStatuses: [],
      isLoading: false,
      error: nodesErr,
    });
    spyUseLiveness.mockReturnValue({
      livenesses: [],
      statuses: {},
      isLoading: false,
      error: livenessErr,
    });

    const { result } = renderHook(() => useNodesSummary());
    expect(result.current.error).toBe(nodesErr);
  });

  it("should return empty summary while still loading", () => {
    spyUseNodes.mockReturnValue({
      nodeStatuses: [makeNodeStatus(1, "addr1:26257", [10])],
      isLoading: true,
      error: undefined,
    });
    spyUseLiveness.mockReturnValue({
      livenesses: [],
      statuses: {},
      isLoading: true,
      error: undefined,
    });

    const { result } = renderHook(() => useNodesSummary());
    expect(result.current.isLoading).toBe(true);
    expect(result.current.nodeStatuses).toEqual([]);
    expect(result.current.nodeIDs).toEqual([]);
    expect(result.current.nodeStatusByID).toEqual({});
    expect(result.current.nodeDisplayNameByID).toEqual({});
    expect(result.current.livenessByNodeID).toEqual({});
    expect(result.current.storeIDsByNodeID).toEqual({});
  });

  it("should handle nodes with no store_statuses", () => {
    const ns = {
      desc: {
        node_id: 1,
        address: { address_field: "addr:26257" },
      },
    } as INodeStatus;

    spyUseNodes.mockReturnValue({
      nodeStatuses: [ns],
      isLoading: false,
      error: undefined,
    });
    spyUseLiveness.mockReturnValue({
      livenesses: [],
      statuses: {},
      isLoading: false,
      error: undefined,
    });

    const { result } = renderHook(() => useNodesSummary());
    expect(result.current.storeIDsByNodeID["1"]).toEqual([]);
  });
});
