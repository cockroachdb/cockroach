// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { render, waitFor, act } from "@testing-library/react";
import React from "react";
import { SWRConfig } from "swr";

import { cockroach } from "src/js/protos";
import * as api from "src/util/api";

import { fetchAllHotRanges, useHotRanges } from "./useHotRanges";

jest.mock("src/util/api", () => ({
  getHotRanges: jest.fn(),
}));

const mockGetHotRanges = api.getHotRanges as jest.MockedFunction<
  typeof api.getHotRanges
>;

type HotRange = cockroach.server.serverpb.HotRangesResponseV2.IHotRange;

function makeRange(overrides: Partial<HotRange> = {}): HotRange {
  return {
    range_id: 1,
    qps: 10,
    node_id: 1,
    databases: ["db1"],
    tables: ["t1"],
    indexes: ["idx1"],
    replica_node_ids: [1],
    leaseholder_node_id: 1,
    store_id: 1,
    ...overrides,
  };
}

function makeResponse(
  ranges: HotRange[],
  nextPageToken: string,
): cockroach.server.serverpb.HotRangesResponseV2 {
  return new cockroach.server.serverpb.HotRangesResponseV2({
    ranges,
    next_page_token: nextPageToken,
  });
}

describe("fetchAllHotRanges", () => {
  beforeEach(() => {
    mockGetHotRanges.mockReset();
  });

  it("fetches a single page when token matches", async () => {
    const range = makeRange({ range_id: 1, qps: 5 });
    mockGetHotRanges.mockResolvedValueOnce(makeResponse([range], ""));

    const result = await fetchAllHotRanges(["1"]);

    expect(result.ranges).toHaveLength(1);
    expect(result.ranges[0].range_id).toBe(1);
    expect(mockGetHotRanges).toHaveBeenCalledTimes(1);
  });

  it("fetches multiple pages until token matches", async () => {
    const r1 = makeRange({ range_id: 1, qps: 10 });
    const r2 = makeRange({ range_id: 2, qps: 20 });
    const r3 = makeRange({ range_id: 3, qps: 30 });

    mockGetHotRanges
      .mockResolvedValueOnce(makeResponse([r1], "page2"))
      .mockResolvedValueOnce(makeResponse([r2], "page3"))
      .mockResolvedValueOnce(makeResponse([r3], "page3"));

    const result = await fetchAllHotRanges(["1", "2"]);

    expect(result.ranges).toHaveLength(3);
    expect(mockGetHotRanges).toHaveBeenCalledTimes(3);
  });

  it("filters out ranges with qps <= 0", async () => {
    const good = makeRange({ range_id: 1, qps: 5 });
    const zeroQps = makeRange({ range_id: 2, qps: 0 });
    const nullQps = makeRange({ range_id: 3, qps: null });
    mockGetHotRanges.mockResolvedValueOnce(
      makeResponse([good, zeroQps, nullQps], ""),
    );

    const result = await fetchAllHotRanges(["1"]);

    expect(result.ranges).toHaveLength(1);
    expect(result.ranges[0].range_id).toBe(1);
  });

  it("propagates API errors", async () => {
    mockGetHotRanges.mockRejectedValueOnce(new Error("network error"));

    await expect(fetchAllHotRanges(["1"])).rejects.toThrow("network error");
  });

  it("rejects if a mid-pagination call fails", async () => {
    const r1 = makeRange({ range_id: 1, qps: 10 });
    mockGetHotRanges
      .mockResolvedValueOnce(makeResponse([r1], "page2"))
      .mockRejectedValueOnce(new Error("timeout on page 2"));

    await expect(fetchAllHotRanges(["1"])).rejects.toThrow("timeout on page 2");
    expect(mockGetHotRanges).toHaveBeenCalledTimes(2);
  });
});

describe("useHotRanges", () => {
  let captured: ReturnType<typeof useHotRanges> | null = null;

  function TestConsumer({ nodeIds }: { nodeIds: number[] }) {
    const result = useHotRanges(nodeIds);
    captured = result;
    return React.createElement("div");
  }

  function renderWithSWR(nodeIds: number[]) {
    return render(
      React.createElement(
        SWRConfig,
        { value: { provider: () => new Map() } },
        React.createElement(TestConsumer, { nodeIds }),
      ),
    );
  }

  beforeEach(() => {
    mockGetHotRanges.mockReset();
    captured = null;
  });

  it("returns empty array when nodeIds is empty", () => {
    renderWithSWR([]);

    expect(captured.hotRanges).toEqual([]);
    expect(captured.isLoading).toBe(false);
    expect(mockGetHotRanges).not.toHaveBeenCalled();
  });

  it("fetches data when nodeIds are provided", async () => {
    const range = makeRange({ range_id: 42, qps: 100 });
    mockGetHotRanges.mockResolvedValueOnce(makeResponse([range], ""));

    await act(async () => {
      renderWithSWR([1]);
    });

    await waitFor(() => {
      expect(captured.hotRanges).toHaveLength(1);
      expect(captured.hotRanges[0].range_id).toBe(42);
      expect(captured.isLoading).toBe(false);
      expect(captured.lastSetAt).toBeDefined();
    });
  });

  it("surfaces API errors via the error field", async () => {
    mockGetHotRanges.mockRejectedValueOnce(new Error("server down"));

    await act(async () => {
      renderWithSWR([1]);
    });

    await waitFor(() => {
      expect(captured.error).toBeDefined();
      expect(captured.isLoading).toBe(false);
      expect(captured.hotRanges).toEqual([]);
    });
  });

  it("refetches when nodeIds change", async () => {
    const r1 = makeRange({ range_id: 1, qps: 10 });
    const r2 = makeRange({ range_id: 2, qps: 20 });

    mockGetHotRanges
      .mockResolvedValueOnce(makeResponse([r1], ""))
      .mockResolvedValueOnce(makeResponse([r2], ""));

    const { rerender } = render(
      React.createElement(
        SWRConfig,
        { value: { provider: () => new Map() } },
        React.createElement(TestConsumer, { nodeIds: [1] }),
      ),
    );

    await waitFor(() => {
      expect(captured.hotRanges).toHaveLength(1);
      expect(captured.hotRanges[0].range_id).toBe(1);
    });

    await act(async () => {
      rerender(
        React.createElement(
          SWRConfig,
          { value: { provider: () => new Map() } },
          React.createElement(TestConsumer, { nodeIds: [2] }),
        ),
      );
    });

    await waitFor(() => {
      expect(captured.hotRanges).toHaveLength(1);
      expect(captured.hotRanges[0].range_id).toBe(2);
    });

    expect(mockGetHotRanges).toHaveBeenCalledTimes(2);
  });
});
